/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package logic

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sjmudd/stopwatch"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/process"
	"vitess.io/vitess/go/vt/vtorc/util"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	discoveriesCounter                 = stats.NewCounter("DiscoveriesAttempt", "Number of discoveries attempted")
	failedDiscoveriesCounter           = stats.NewCounter("DiscoveriesFail", "Number of failed discoveries")
	instancePollSecondsExceededCounter = stats.NewCounter("DiscoveriesInstancePollSecondsExceeded", "Number of instances that took longer than InstancePollSeconds to poll")
	discoveryQueueLengthGauge          = stats.NewGauge("DiscoveriesQueueLength", "Length of the discovery queue")
	discoveryRecentCountGauge          = stats.NewGauge("DiscoveriesRecentCount", "Number of recent discoveries")
	discoveryWorkersGauge              = stats.NewGauge("DiscoveryWorkers", "Number of discovery workers")
	discoveryWorkersActiveGauge        = stats.NewGauge("DiscoveryWorkersActive", "Number of discovery workers actively discovering tablets")

	discoveryInstanceTimingsActions = []string{"Backend", "Instance", "Other"}
	discoveryInstanceTimings        = stats.NewTimings("DiscoveryInstanceTimings", "Timings for instance discovery actions", "Action", discoveryInstanceTimingsActions...)
)

// VTOrc represents an instance of VTOrc.
type VTOrc struct {
	discoveryQueue               *DiscoveryQueue
	hasReceivedSIGTERM           int32
	metricsTickCancel            context.CancelFunc
	recentDiscoveryOperationKeys *cache.Cache
	tmc                          tmclient.TabletManagerClient
	ts                           *topo.Server

	// shardsLockCounter is a count of in-flight shard locks. Use atomics to read/update.
	shardsLockCounter int64

	// shardsToWatch is a map storing the shards for a given keyspace that need to be watched.
	// We store the key range for all the shards that we want to watch.
	// This is populated by parsing `--clusters_to_watch` flag.
	shardsToWatch map[string][]*topodatapb.KeyRange

	// TODO: drop in v25.
	snapshotDiscoveryKeys      chan string
	snapshotDiscoveryKeysMutex sync.Mutex
}

// NewVTOrc inits a new *VTOrc.
func NewVTOrc(ts *topo.Server) (*VTOrc, error) {
	if config.GetCell() == "" {
		// TODO: remove warning in v25+, make flag required.
		log.Warning("WARNING: --cell will become a required vtorc flag in v25 and up")
	}

	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	_, err := ts.GetCellInfo(ctx, config.GetCell(), true /* strongRead */)
	if err != nil {
		return nil, err
	}

	// Parse --clusters_to_watch into a filter.
	shardsToWatch, err := buildShardsToWatch()
	if err != nil {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Error parsing --clusters-to-watch: %v", err)
	}

	vtorc := &VTOrc{
		discoveryQueue:               NewDiscoveryQueue(),
		recentDiscoveryOperationKeys: cache.New(config.GetInstancePollTime(), time.Second),
		shardsToWatch:                shardsToWatch,
		snapshotDiscoveryKeys:        make(chan string, 10),
		tmc:                          inst.InitializeTMC(),
		ts:                           ts,
	}
	vtorc.initMetrics()
	vtorc.initTopologyRecoveryMetrics()
	return vtorc, nil
}

// Close runs all the operations required to cleanly shutdown VTOrc
func (vtorc *VTOrc) Close() {
	log.Infof("Starting VTOrc shutdown")
	atomic.StoreInt32(&vtorc.hasReceivedSIGTERM, 1)
	// Poke other go routines to stop cleanly here ...
	_ = inst.AuditOperation("shutdown", "", "Triggered via SIGTERM")
	// Reset and cancel metrics ticks
	resetMetricsTicks()
	vtorc.metricsTickCancel()
	// Wait for the locks to be released
	vtorc.waitForLocksRelease()
	// Close clients
	vtorc.tmc.Close()
	vtorc.ts.Close()
	log.Infof("VTOrc closed")
}

func (vtorc *VTOrc) initMetrics() {
	onMetricsTick(func() {
		discoveryQueueLengthGauge.Set(int64(vtorc.discoveryQueue.QueueLen()))
	})
	onMetricsTick(func() {
		if vtorc.recentDiscoveryOperationKeys == nil {
			return
		}
		discoveryRecentCountGauge.Set(int64(vtorc.recentDiscoveryOperationKeys.ItemCount()))
	})
}

// waitForLocksRelease is used to wait for release of locks
func (vtorc *VTOrc) waitForLocksRelease() {
	for {
		select {
		case <-time.After(shutdownWaitTime):
			log.Infof("Wait for lock release timed out at %s. Some locks might not have been released.", shutdownWaitTime)
		default:
			count := atomic.LoadInt64(&vtorc.shardsLockCounter)
			if count == 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
	}
}

// handleDiscoveryRequests iterates the discoveryQueue channel and calls upon
// instance discovery per entry.
func (vtorc *VTOrc) handleDiscoveryRequests() {
	if vtorc.discoveryQueue == nil {
		return
	}
	// create a pool of discovery workers
	for i := uint(0); i < config.GetDiscoveryWorkers(); i++ {
		discoveryWorkersGauge.Add(1)
		go func() {
			for {
				// .Consume() blocks until there is a new key to process.
				// We are not "active" until we got a tablet alias.
				tabletAlias := vtorc.discoveryQueue.Consume()
				func() {
					discoveryWorkersActiveGauge.Add(1)
					defer discoveryWorkersActiveGauge.Add(-1)

					vtorc.DiscoverInstance(tabletAlias, false /* forceDiscovery */)
					vtorc.discoveryQueue.Release(tabletAlias)
				}()
			}
		}()
	}
}

// DiscoverInstance will attempt to discover (poll) an instance (unless
// it is already up-to-date) and will also ensure that its primary and
// replicas (if any) are also checked.
func (vtorc *VTOrc) DiscoverInstance(tabletAlias string, forceDiscovery bool) {
	if inst.InstanceIsForgotten(tabletAlias) {
		log.Infof("DiscoverInstance: skipping discovery of %+v because it is set to be forgotten", tabletAlias)
		return
	}

	// create stopwatch entries
	latency := stopwatch.NewNamedStopwatch()
	_ = latency.AddMany([]string{
		"backend",
		"instance",
		"total"})
	latency.Start("total") // start the total stopwatch (not changed anywhere else)
	defer func() {
		latency.Stop("total")
		discoveryTime := latency.Elapsed("total")
		if discoveryTime > config.GetInstancePollTime() {
			instancePollSecondsExceededCounter.Add(1)
			log.Warningf("DiscoverInstance exceeded InstancePollSeconds for %+v, took %.4fs", tabletAlias, discoveryTime.Seconds())
		}
	}()

	if tabletAlias == "" {
		return
	}

	// Calculate the expiry period each time as InstancePollSeconds
	// _may_ change during the run of the process (via SIGHUP) and
	// it is not possible to change the cache's default expiry..
	if existsInCacheError := vtorc.recentDiscoveryOperationKeys.Add(tabletAlias, true, config.GetInstancePollTime()); existsInCacheError != nil && !forceDiscovery {
		// Just recently attempted
		return
	}

	latency.Start("backend")
	instance, found, _ := inst.ReadInstance(tabletAlias)
	latency.Stop("backend")
	if !forceDiscovery && found && instance.IsUpToDate && instance.IsLastCheckValid {
		// we've already discovered this one. Skip!
		return
	}

	discoveriesCounter.Add(1)

	// First we've ever heard of this instance. Continue investigation:
	instance, err := inst.ReadTopologyInstanceBufferable(tabletAlias, latency)
	// panic can occur (IO stuff). Therefore it may happen
	// that instance is nil. Check it, but first get the timing metrics.
	totalLatency := latency.Elapsed("total")
	backendLatency := latency.Elapsed("backend")
	instanceLatency := latency.Elapsed("instance")
	otherLatency := totalLatency - (backendLatency + instanceLatency)

	discoveryInstanceTimings.Add("Backend", backendLatency)
	discoveryInstanceTimings.Add("Instance", instanceLatency)
	discoveryInstanceTimings.Add("Other", otherLatency)

	if err != nil {
		log.Errorf("Failed to discover %s (force: %t), err: %v", tabletAlias, forceDiscovery, err)
	} else {
		log.Infof("Discovered %s (force: %t): %+v", tabletAlias, forceDiscovery, instance)
	}

	if instance == nil {
		failedDiscoveriesCounter.Add(1)
		if util.ClearToLog("discoverInstance", tabletAlias) {
			log.Warningf("DiscoverInstance(%+v) instance is nil in %.3fs (Backend: %.3fs, Instance: %.3fs), error=%+v",
				tabletAlias,
				totalLatency.Seconds(),
				backendLatency.Seconds(),
				instanceLatency.Seconds(),
				err)
		}
		return
	}
}

// onHealthTick handles the actions to take to discover/poll instances
func (vtorc *VTOrc) onHealthTick() {
	tabletAliases, err := inst.ReadOutdatedInstanceKeys()
	if err != nil {
		log.Error(err)
	}

	func() {
		// Normally onHealthTick() shouldn't run concurrently. It is kicked by a ticker.
		// However it _is_ invoked inside a goroutine. I like to be safe here.
		vtorc.snapshotDiscoveryKeysMutex.Lock()
		defer vtorc.snapshotDiscoveryKeysMutex.Unlock()

		countSnapshotKeys := len(vtorc.snapshotDiscoveryKeys)
		for i := 0; i < countSnapshotKeys; i++ {
			tabletAliases = append(tabletAliases, <-vtorc.snapshotDiscoveryKeys)
		}
	}()
	// avoid any logging unless there's something to be done
	if len(tabletAliases) > 0 {
		for _, tabletAlias := range tabletAliases {
			if tabletAlias != "" {
				vtorc.discoveryQueue.Push(tabletAlias)
			}
		}
	}
}

// ContinuousDiscovery starts an asynchronous infinite discovery process where instances are
// periodically investigated and their status captured, and long since unseen instances are
// purged and forgotten.
func (vtorc *VTOrc) ContinuousDiscovery() {
	log.Info("Continuous discovery: setting up")

	if !config.GetAllowRecovery() {
		log.Info("--allow-recovery is set to 'false', disabling recovery actions")
		if err := DisableRecovery(); err != nil {
			log.Errorf("failed to disable recoveries: %+v", err)
			return
		}
	}

	go vtorc.handleDiscoveryRequests()

	ctx := context.Background()
	healthTick := time.Tick(config.HealthPollSeconds * time.Second)
	caretakingTick := time.Tick(time.Minute)
	recoveryTick := time.Tick(config.GetRecoveryPollDuration())
	tabletTopoTick := vtorc.OpenTabletDiscovery(ctx)
	var recoveryEntrance int64
	var snapshotTopologiesTick <-chan time.Time
	if config.GetSnapshotTopologyInterval() > 0 {
		snapshotTopologiesTick = time.Tick(config.GetSnapshotTopologyInterval())
	}

	var metricsTickCtx context.Context
	metricsTickCtx, vtorc.metricsTickCancel = context.WithCancel(ctx)
	go func() {
		_ = initMetricsTick(metricsTickCtx)
	}()
	// On termination of the server, we should close VTOrc cleanly
	servenv.OnTermSync(vtorc.Close)

	log.Info("Continuous discovery: starting")
	for {
		select {
		case <-healthTick:
			go vtorc.onHealthTick()
		case <-caretakingTick:
			// Various periodic internal maintenance tasks
			//nolint:errcheck
			go inst.ForgetLongUnseenInstances()
			go inst.ExpireAudit()
			go inst.ExpireStaleInstanceBinlogCoordinates()
			go ExpireRecoveryDetectionHistory()
			go ExpireTopologyRecoveryHistory()
			go ExpireTopologyRecoveryStepsHistory()
		case <-recoveryTick:
			go func() {
				go inst.ExpireInstanceAnalysisChangelog() //nolint:errcheck

				go func() {
					// This function is non re-entrant (it can only be running once at any point in time)
					if atomic.CompareAndSwapInt64(&recoveryEntrance, 0, 1) {
						defer atomic.StoreInt64(&recoveryEntrance, 0)
					} else {
						return
					}
					ctx, cancel := context.WithTimeout(ctx, config.GetRecoveryPollDuration())
					defer cancel()
					vtorc.CheckAndRecover(ctx)
				}()
			}()
		case <-snapshotTopologiesTick:
			go inst.SnapshotTopologies() //nolint:errcheck
		case <-tabletTopoTick:
			func() {
				ctx, cancel := context.WithTimeout(ctx, config.GetTopoInformationRefreshDuration())
				defer cancel()
				if err := vtorc.refreshAllInformation(ctx); err != nil {
					log.Errorf("Failed to refresh topo information: %+v", err)
				}
			}()
		}
	}
}

// refreshAllInformation refreshes both shard and tablet information. This is meant to be run on tablet topo ticks.
func (vtorc *VTOrc) refreshAllInformation(ctx context.Context) error {
	// Create an errgroup
	eg, egCtx := errgroup.WithContext(ctx)

	// Refresh all keyspace information.
	eg.Go(func() error {
		return vtorc.RefreshAllKeyspacesAndShards(egCtx)
	})

	// Refresh all tablets.
	eg.Go(func() error {
		return vtorc.refreshAllTablets(egCtx)
	})

	// Wait for both the refreshes to complete
	err := eg.Wait()
	if err == nil {
		process.FirstDiscoveryCycleComplete.Store(true)
	}
	return err
}
