/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package txthrottler

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
)

var (
	ErrThrottledConnPoolUsageHard = errors.New("ConnPoolUsageHard")
	ErrThrottledConnPoolUsageSoft = errors.New("ConnPoolUsageSoft")
	ErrThrottledTxPoolUsageHard   = errors.New("TxPoolUsageHard")
	ErrThrottledTxPoolUsageSoft   = errors.New("TxPoolUsageSoft")
	ErrThrottledReplicationLag    = errors.New("ReplicationLag")
)

// These vars store the functions used to create the topo server, healthcheck,
// topology watchers and go/vt/throttler. These are provided here so that they can be overridden
// in tests to generate mocks.
type healthCheckFactoryFunc func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck
type topologyWatcherFactoryFunc func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface
type throttlerFactoryFunc func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error)

var (
	healthCheckFactory     healthCheckFactoryFunc
	topologyWatcherFactory topologyWatcherFactoryFunc
	throttlerFactory       throttlerFactoryFunc
)

func resetTxThrottlerFactories() {
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return discovery.NewHealthCheck(context.Background(), discovery.DefaultHealthCheckRetryDelay, discovery.DefaultHealthCheckTimeout, topoServer, cell, strings.Join(cellsToWatch, ","))
	}
	topologyWatcherFactory = func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		return discovery.NewCellTabletsWatcher(context.Background(), topoServer, hc, discovery.NewFilterByKeyspace([]string{keyspace}), cell, refreshInterval, true, topoReadConcurrency)
	}
	throttlerFactory = func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error) {
		return throttler.NewThrottlerFromConfig(name, unit, threadCount, maxRate, maxReplicationLagConfig, time.Now)
	}
}

// TxThrottler defines the interface for the transaction throttler.
type TxThrottler interface {
	InitDBConfig(target *querypb.Target)
	Open() (err error)
	Close()
	Throttle(plan *planbuilder.Plan, options *querypb.ExecuteOptions) error
}

func init() {
	resetTxThrottlerFactories()
}

// poolUsageInterface defines an interface that provides throttling
// signals based on pool usage.
type poolUsageInterface interface {
	GetPoolUsagePercent() float64
}

// ThrottlerInterface defines the public interface that is implemented by go/vt/throttler.Throttler
// It is only used here to allow mocking out a throttler object.
type ThrottlerInterface interface {
	Throttle(threadID int) time.Duration
	ThreadFinished(threadID int)
	Close()
	MaxRate() int64
	SetMaxRate(rate int64)
	RecordReplicationLag(time time.Time, th *discovery.TabletHealth)
	GetConfiguration() *throttlerdatapb.Configuration
	UpdateConfiguration(configuration *throttlerdatapb.Configuration, copyZeroValues bool) error
	ResetConfiguration()
}

// TopologyWatcherInterface defines the public interface that is implemented by
// discovery.LegacyTopologyWatcher. It is only used here to allow mocking out
// go/vt/discovery.LegacyTopologyWatcher.
type TopologyWatcherInterface interface {
	Start()
	Stop()
}

// TxThrottlerName is the name the wrapped go/vt/throttler object will be registered with
// go/vt/throttler.GlobalManager.
const TxThrottlerName = "TransactionThrottler"

// txThrottler implements TxThrottle for throttling transactions based on replication lag.
// It's a thin wrapper around the throttler found in vitess/go/vt/throttler.
// It uses a discovery.HealthCheck to send replication-lag updates to the wrapped throttler.
//
// Intended Usage:
//
//	// Assuming topoServer is a topo.Server variable pointing to a Vitess topology server.
//	t := NewTxThrottler(config, topoServer)
//
//	// A transaction throttler must be opened before its first use:
//	if err := t.Open(keyspace, shard); err != nil {
//	  return err
//	}
//
//	// Checking whether to throttle can be done as follows before starting a transaction.
//	if err := t.Throttle(); err != nil {
//	  return fmt.Errorf("Transaction throttled: %w", err)
//	} else {
//	  // execute transaction.
//	}
//
//	// To release the resources used by the throttler the caller should call Close().
//	t.Close()
//
// A txThrottler object is generally not thread-safe: at any given time at most one goroutine should
// be executing a method. The only exception is the 'Throttle' method where multiple goroutines are
// allowed to execute it concurrently.
type txThrottler struct {
	// config stores the transaction throttler's configuration.
	// It is populated in NewTxThrottler and is not modified
	// since.
	config *txThrottlerConfig

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state *txThrottlerState

	// engines
	queryEngine poolUsageInterface
	txEngine    poolUsageInterface

	target     *querypb.Target
	topoServer *topo.Server

	// stats
	throttlerRunning  *stats.Gauge
	requestsTotal     *stats.CountersWithSingleLabel
	requestsThrottled *stats.CountersWithMultiLabels
}

// txThrottlerConfig holds the parameters that need to be
// passed when constructing a TxThrottler object.
type txThrottlerConfig struct {
	// enabled is true if the transaction throttler is enabled. All methods
	// of a disabled transaction throttler do nothing and Throttle() always
	// returns false.
	enabled bool

	throttlerConfig *throttlerdatapb.Configuration
	// healthCheckCells stores the cell names in which running vttablets will be monitored for
	// replication lag.
	healthCheckCells []string

	// tabletTypes stores the tablet types for throttling
	tabletTypes *topoproto.TabletTypeListFlag

	// defaultPriority is the default priority of a transaction/query
	defaultPriority int

	// queryPoolThresholds and txPoolThresholds are pool usage threshold to throttle all low-priority queries/transactions
	queryPoolThresholds, txPoolThresholds *flagutil.StringLowHighPercentValues
}

// txThrottlerState holds the state of an open TxThrottler object.
type txThrottlerState struct {
	config *txThrottlerConfig

	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu      sync.Mutex
	throttler       ThrottlerInterface
	stopHealthCheck context.CancelFunc
	queryEngine     poolUsageInterface
	txEngine        poolUsageInterface

	healthCheck      discovery.HealthCheck
	topologyWatchers []TopologyWatcherInterface
}

// NewTxThrottler tries to construct a txThrottler from the
// relevant fields in the tabletenv.Config object. It returns a disabled TxThrottler if
// any error occurs.
// This function calls tryCreateTxThrottler that does the actual creation work
// and returns an error if one occurred.
func NewTxThrottler(env tabletenv.Env, topoServer *topo.Server, queryEngine, txEngine poolUsageInterface) TxThrottler {
	throttlerConfig := &txThrottlerConfig{enabled: false}

	if env.Config().EnableTxThrottler {
		// Clone tsv.TxThrottlerHealthCheckCells so that we don't assume tsv.TxThrottlerHealthCheckCells
		// is immutable.
		healthCheckCells := env.Config().TxThrottlerHealthCheckCells

		throttlerConfig = &txThrottlerConfig{
			enabled:             true,
			defaultPriority:     env.Config().TxThrottlerDefaultPriority,
			queryPoolThresholds: env.Config().TxThrottlerQueryPoolThresholds,
			txPoolThresholds:    env.Config().TxThrottlerTxPoolThresholds,
			tabletTypes:         env.Config().TxThrottlerTabletTypes,
			throttlerConfig:     env.Config().TxThrottlerConfig.Get(),
			healthCheckCells:    healthCheckCells,
		}

		defer log.Infof("Initialized transaction throttler with config: %+v", throttlerConfig)
	}

	return &txThrottler{
		config:           throttlerConfig,
		queryEngine:      queryEngine,
		txEngine:         txEngine,
		topoServer:       topoServer,
		throttlerRunning: env.Exporter().NewGauge("TransactionThrottlerRunning", "transaction throttler running state"),
		requestsTotal: env.Exporter().NewCountersWithSingleLabel("TransactionThrottlerRequests", "transaction throttler requests",
			"type",
		),
		requestsThrottled: env.Exporter().NewCountersWithMultiLabels("TransactionThrottlerThrottled", "transaction throttler requests throttled",
			[]string{"type", "reason"},
		),
	}
}

// getPriorityFromOptions returns the priority of an operation as an integer between 0 and
// 100. The defaultPriority is returned if none is found in *querypb.ExecuteOptions.
func (t *txThrottler) getPriorityFromOptions(options *querypb.ExecuteOptions) int {
	priority := t.config.defaultPriority
	if options == nil {
		return priority
	}
	if options.Priority == "" {
		return priority
	}

	optionsPriority, err := strconv.Atoi(options.Priority)
	// This should never error out, as the value for Priority has been validated in the vtgate already.
	// Still, handle it just to make sure.
	if err != nil {
		log.Errorf(
			"The value of the %s query directive could not be converted to integer, using the "+
				"default value. Error was: %s",
			sqlparser.DirectivePriority, priority, err)

		return priority
	}

	return optionsPriority
}

// InitDBConfig initializes the target parameters for the throttler.
func (t *txThrottler) InitDBConfig(target *querypb.Target) {
	t.target = proto.Clone(target).(*querypb.Target)
}

// Open opens the transaction throttler. It must be called prior to 'Throttle'.
func (t *txThrottler) Open() (err error) {
	if !t.config.enabled {
		return nil
	}
	if t.state != nil {
		return nil
	}
	log.Info("txThrottler: opening")
	t.throttlerRunning.Set(1)
	t.state, err = newTxThrottlerState(t.topoServer, t.queryEngine, t.txEngine, t.config, t.target)
	return err
}

// Close closes the txThrottler object and releases resources.
// It should be called after the throttler is no longer needed.
// It's ok to call this method on a closed throttler--in which case the method does nothing.
func (t *txThrottler) Close() {
	if !t.config.enabled {
		return
	}
	if t.state == nil {
		return
	}
	t.state.deallocateResources()
	t.state = nil
	t.throttlerRunning.Set(0)
	log.Info("txThrottler: closed")
}

// Throttle should be called before a new transaction is started. It
// returns an error if the transaction should not proceed (the caller
// should back off). Throttle requires that Open() was previously
// called successfully.
func (t *txThrottler) Throttle(plan *planbuilder.Plan, options *querypb.ExecuteOptions) error {
	if !t.config.enabled {
		return nil
	}
	if t.state == nil {
		return nil
	}

	throttleErr := t.state.throttle(plan)
	t.requestsTotal.Add(plan.PlanID.String(), 1)
	if throttleErr == nil {
		return nil
	}

	// Throttle according to both what the throttler state says and the priority. Workloads with lower priority value
	// are less likely to be throttled.
	throttledLabels := []string{plan.PlanID.String(), throttleErr.Error()}
	switch throttleErr {
	case ErrThrottledConnPoolUsageHard:
		t.requestsThrottled.Add(throttledLabels, 1)
		return throttleErr
	case ErrThrottledConnPoolUsageSoft, ErrThrottledReplicationLag:
		priority := t.getPriorityFromOptions(options)
		if priority == 0 {
			return nil
		}
		if rand.Intn(sqlparser.MaxPriorityValue) < priority {
			t.requestsThrottled.Add(throttledLabels, 1)
			return throttleErr
		}
	}
	return nil
}

func newTxThrottlerState(topoServer *topo.Server, queryEngine, txEngine poolUsageInterface, config *txThrottlerConfig, target *querypb.Target) (*txThrottlerState, error) {
	maxReplicationLagModuleConfig := throttler.MaxReplicationLagModuleConfig{Configuration: config.throttlerConfig}

	t, err := throttlerFactory(
		TxThrottlerName,
		"TPS",                           /* unit */
		1,                               /* threadCount */
		throttler.MaxRateModuleDisabled, /* maxRate */
		maxReplicationLagModuleConfig,
	)
	if err != nil {
		return nil, err
	}
	if err := t.UpdateConfiguration(config.throttlerConfig, true /* copyZeroValues */); err != nil {
		t.Close()
		return nil, err
	}
	result := &txThrottlerState{
		config:      config,
		queryEngine: queryEngine,
		txEngine:    txEngine,
		throttler:   t,
	}
	createTxThrottlerHealthCheck(topoServer, config, result, target.Cell)

	result.topologyWatchers = make(
		[]TopologyWatcherInterface, 0, len(config.healthCheckCells))
	for _, cell := range config.healthCheckCells {
		result.topologyWatchers = append(
			result.topologyWatchers,
			topologyWatcherFactory(
				topoServer,
				result.healthCheck,
				cell,
				target.Keyspace,
				target.Shard,
				discovery.DefaultTopologyWatcherRefreshInterval,
				discovery.DefaultTopoReadConcurrency))
	}
	return result, nil
}

func createTxThrottlerHealthCheck(topoServer *topo.Server, config *txThrottlerConfig, result *txThrottlerState, cell string) {
	ctx, cancel := context.WithCancel(context.Background())
	result.stopHealthCheck = cancel
	result.healthCheck = healthCheckFactory(topoServer, cell, config.healthCheckCells)
	ch := result.healthCheck.Subscribe()
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case th := <-ch:
				result.StatsUpdate(th)
			}
		}
	}(ctx)
}

func checkPoolUsage(engine poolUsageInterface, thresholds *flagutil.StringLowHighPercentValues, highErr, lowErr error) error {
	// Calls to .GetPoolUsagePercent() are serialized by the underlying engine.
	switch usagePercent := engine.GetPoolUsagePercent(); {
	case thresholds.High > 0 && usagePercent >= thresholds.High:
		return highErr
	case thresholds.Low > 0 && usagePercent >= thresholds.Low:
		return lowErr
	default:
		return nil
	}
}

func (ts *txThrottlerState) throttle(plan *planbuilder.Plan) error {
	if ts.throttler == nil {
		log.Error("txThrottler: throttle called after deallocateResources was called")
		return nil
	}
	if plan == nil {
		log.Error("txThrottler: throttle called with no plan")
		return nil
	}
	switch plan.PlanID {
	case planbuilder.PlanSelect, planbuilder.PlanSelectImpossible, planbuilder.PlanShow:
		if err := checkPoolUsage(ts.queryEngine, ts.config.queryPoolThresholds, ErrThrottledConnPoolUsageHard,
			ErrThrottledConnPoolUsageSoft); err != nil {
			return err
		}
	default:
		// check tx pool usage
		if err := checkPoolUsage(ts.txEngine, ts.config.txPoolThresholds, ErrThrottledTxPoolUsageHard,
			ErrThrottledTxPoolUsageSoft); err != nil {
			return err
		}

		// Serialize calls to ts.throttle.Throttle().
		ts.throttleMu.Lock()
		defer ts.throttleMu.Unlock()
		if ts.throttler.Throttle(0 /* threadId */) > 0 {
			return ErrThrottledReplicationLag
		}
	}
	return nil
}

func (ts *txThrottlerState) deallocateResources() {
	// We don't really need to nil out the fields here
	// as deallocateResources is not expected to be called
	// more than once, but it doesn't hurt to do so.
	for _, watcher := range ts.topologyWatchers {
		watcher.Stop()
	}
	ts.topologyWatchers = nil

	ts.healthCheck.Close()
	ts.healthCheck = nil

	// After ts.healthCheck is closed txThrottlerState.StatsUpdate() is guaranteed not
	// to be executing, so we can safely close the throttler.
	ts.throttler.Close()
	ts.throttler = nil
}

// StatsUpdate updates the health of a tablet with the given healthcheck.
func (ts *txThrottlerState) StatsUpdate(tabletStats *discovery.TabletHealth) {
	if ts.config.tabletTypes == nil {
		return
	}

	// Monitor tablets for replication lag if they have a tablet
	// type specified by the --tx_throttler_tablet_types flag.
	for _, expectedTabletType := range *ts.config.tabletTypes {
		if tabletStats.Target.TabletType == expectedTabletType {
			ts.throttler.RecordReplicationLag(time.Now(), tabletStats)
			return
		}
	}
}
