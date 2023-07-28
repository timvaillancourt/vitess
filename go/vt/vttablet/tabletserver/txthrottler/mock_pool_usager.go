package txthrottler

type mockPoolUsager struct {
	poolUsage float64
}

func newMockPoolUsager() *mockPoolUsager {
	return &mockPoolUsager{}
}

func (qe *mockPoolUsager) SetPoolUsagePercent(pu float64) {
	qe.poolUsage = pu
}

func (qe *mockPoolUsager) GetPoolUsagePercent() float64 {
	return qe.poolUsage
}
