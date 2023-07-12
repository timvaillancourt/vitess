package tabletserver

import (
	"math/rand"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type QueryThrottler interface {
	Throttle(priority int) (result bool)
}

type queryThrottler struct {
	config *tabletenv.TabletConfig
	qe     queryEngine

	enabled bool
}

func NewQueryThrottler(env tabletenv.Env, qe queryEngine) QueryThrottler {
	config := env.Config()
	return &queryThrottler{
		config:  config,
		enabled: config.QueryThrottlerPoolThreshold > 0,
		qe:      qe,
	}
}

func (qt *queryThrottler) Throttle(priority int) (result bool) {
	if !qt.enabled {
		return false
	}
	if int(qt.qe.GetConnPoolUsagePercent()) > qt.config.QueryThrottlerPoolThreshold {
		result = rand.Intn(sqlparser.MaxPriorityValue) < priority
	}
	return result
}
