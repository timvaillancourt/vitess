package txthrottler

type mockQueryEngine struct {
	poolUsage float64
}

func newMockQueryEngine() *mockQueryEngine {
	return &mockQueryEngine{}
}

func (qe *mockQueryEngine) SetConnPoolUsagePercent(pu float64) {
	qe.poolUsage = pu
}

func (qe *mockQueryEngine) GetConnPoolUsagePercent() float64 {
	return qe.poolUsage
}
