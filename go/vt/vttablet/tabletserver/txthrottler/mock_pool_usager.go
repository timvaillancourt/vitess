package txthrottler

type mockPoolUsager struct {
	poolUsage float64
}

func newMockPoolUsager() *mockPoolUsager {
	return &mockPoolUsager{}
}

func (qe *mockPoolUsager) setPoolUsagePercent(p float64) {
	qe.poolUsage = p
}

func (qe *mockPoolUsager) GetPoolUsagePercent() float64 {
	return qe.poolUsage
}
