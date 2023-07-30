package txthrottler

type mockPoolUsager struct {
	poolUsage float64
}

func newMockPoolUsager() *mockPoolUsager {
	return &mockPoolUsager{}
}

func (qe *mockPoolUsager) setPoolUsagePercent(pu float64) {
	qe.poolUsage = pu
}

func (qe *mockPoolUsager) GetPoolUsagePercent() float64 {
	return qe.poolUsage
}
