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

// Commands to generate the mocks for this test.
//go:generate mockgen -destination mock_healthcheck_test.go -package txthrottler -mock_names "HealthCheck=MockHealthCheck" vitess.io/vitess/go/vt/discovery HealthCheck
//go:generate mockgen -destination mock_throttler_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler ThrottlerInterface
//go:generate mockgen -destination mock_topology_watcher_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler TopologyWatcherInterface

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDisabledThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = false
	env := tabletenv.NewEnv(config, t.Name())
	throttler := NewTxThrottler(env, nil, newMockPoolUsager(), newMockPoolUsager())
	throttler.InitDBConfig(&querypb.Target{
		Keyspace: "keyspace",
		Shard:    "shard",
	})
	assert.Nil(t, throttler.Open())
	assert.Nil(t, throttler.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "0"},
	))
	throttlerImpl, _ := throttler.(*txThrottler)
	assert.Zero(t, throttlerImpl.throttlerRunning.Get())
	throttler.Close()
}

func TestEnabledThrottler(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	defer resetTxThrottlerFactories()
	ts := memorytopo.NewServer("cell1", "cell2")

	mockHealthCheck := NewMockHealthCheck(mockCtrl)
	hcCall1 := mockHealthCheck.EXPECT().Subscribe()
	hcCall1.Do(func() {})
	hcCall2 := mockHealthCheck.EXPECT().Close()
	hcCall2.After(hcCall1)
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return mockHealthCheck
	}

	topologyWatcherFactory = func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		assert.Equal(t, ts, topoServer)
		assert.Contains(t, []string{"cell1", "cell2"}, cell)
		assert.Equal(t, "keyspace", keyspace)
		assert.Equal(t, "shard", shard)
		result := NewMockTopologyWatcherInterface(mockCtrl)
		result.EXPECT().Stop()
		return result
	}

	mockThrottler := NewMockThrottlerInterface(mockCtrl)
	throttlerFactory = func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error) {
		assert.Equal(t, 1, threadCount)
		return mockThrottler, nil
	}

	call0 := mockThrottler.EXPECT().UpdateConfiguration(gomock.Any(), true /* copyZeroValues */)
	call1 := mockThrottler.EXPECT().Throttle(0)
	call1.Return(0 * time.Second)
	tabletStats := &discovery.TabletHealth{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}
	call2 := mockThrottler.EXPECT().RecordReplicationLag(gomock.Any(), tabletStats)
	call3 := mockThrottler.EXPECT().Throttle(0)
	call3.Return(1 * time.Second)

	call4 := mockThrottler.EXPECT().Throttle(0)
	call4.Return(1 * time.Second)
	calllast := mockThrottler.EXPECT().Close()

	call1.After(call0)
	call2.After(call1)
	call3.After(call2)
	call4.After(call3)
	calllast.After(call4)

	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = true
	config.TxThrottlerHealthCheckCells = []string{"cell1", "cell2"}
	config.TxThrottlerTabletTypes = &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA}
	config.TxThrottlerQueryPoolThresholds = &flagutil.StringLowHighPercentValues{Low: 66.66, High: 80}
	config.TxThrottlerTxPoolThresholds = &flagutil.StringLowHighPercentValues{Low: 66.66, High: 80}

	env := tabletenv.NewEnv(config, t.Name())
	queryEngine := newMockPoolUsager()
	txEngine := newMockPoolUsager()
	throttler := NewTxThrottler(env, ts, queryEngine, txEngine)
	throttlerImpl, _ := throttler.(*txThrottler)
	assert.NotNil(t, throttlerImpl)
	throttler.InitDBConfig(&querypb.Target{
		Keyspace: "keyspace",
		Shard:    "shard",
	})
	assert.Nil(t, throttlerImpl.Open())
	assert.Equal(t, int64(1), throttlerImpl.throttlerRunning.Get())

	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanBegin},
		&querypb.ExecuteOptions{Priority: "100"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String(): 1,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Len(t, throttlerImpl.requestsThrottled.Counts(), 0)

	throttlerImpl.state.StatsUpdate(tabletStats) // This calls replication lag thing
	rdonlyTabletStats := &discovery.TabletHealth{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_RDONLY,
		},
	}
	// This call should not be forwarded to the go/vt/throttlerImpl.Throttler object.
	throttlerImpl.state.StatsUpdate(rdonlyTabletStats)
	// The second throttle call should reject.
	assert.ErrorIs(t, ErrThrottledReplicationLag, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "100"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String():  1,
		planbuilder.PlanInsert.String(): 1,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error(): 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// This call should not throttle due to priority. Check that's the case and counters agree.
	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "0"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String():  1,
		planbuilder.PlanInsert.String(): 2,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error(): 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test select + query conn pool signal, which is below threshold. This call should not throttle.
	queryEngine.SetPoolUsagePercent(12.345)
	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanSelect},
		&querypb.ExecuteOptions{Priority: "100"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String():  1,
		planbuilder.PlanInsert.String(): 2,
		planbuilder.PlanSelect.String(): 1,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error(): 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test select + query conn pool signal, which is above the "soft" threshold. This call should throttle.
	queryEngine.SetPoolUsagePercent(75)
	assert.ErrorIs(t, ErrThrottledConnPoolUsageSoft, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanSelect},
		&querypb.ExecuteOptions{Priority: "100"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String():  1,
		planbuilder.PlanInsert.String(): 2,
		planbuilder.PlanSelect.String(): 2,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error():    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error(): 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test select + query conn pool signal, which is above the "high" threshold. This call should throttle.
	queryEngine.SetPoolUsagePercent(99.999)
	assert.ErrorIs(t, ErrThrottledConnPoolUsageHard, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanSelect},
		&querypb.ExecuteOptions{Priority: "1"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String():  1,
		planbuilder.PlanInsert.String(): 2,
		planbuilder.PlanSelect.String(): 3,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error():    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error(): 1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageHard.Error(): 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Close throttler.
	throttlerImpl.Close()
	assert.Zero(t, throttlerImpl.throttlerRunning.Get())
}

func TestNewTxThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, t.Name())

	{
		// disabled
		config.EnableTxThrottler = false
		throttler := NewTxThrottler(env, nil, newMockPoolUsager(), newMockPoolUsager())
		throttlerImpl, _ := throttler.(*txThrottler)
		assert.NotNil(t, throttlerImpl)
		assert.NotNil(t, throttlerImpl.config)
		assert.False(t, throttlerImpl.config.enabled)
	}
	{
		// enabled
		config.EnableTxThrottler = true
		config.TxThrottlerHealthCheckCells = []string{"cell1", "cell2"}
		config.TxThrottlerTabletTypes = &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA}
		throttler := NewTxThrottler(env, nil, newMockPoolUsager(), newMockPoolUsager())
		throttlerImpl, _ := throttler.(*txThrottler)
		assert.NotNil(t, throttlerImpl)
		assert.NotNil(t, throttlerImpl.config)
		assert.True(t, throttlerImpl.config.enabled)
		assert.Equal(t, []string{"cell1", "cell2"}, throttlerImpl.config.healthCheckCells)
	}
}
