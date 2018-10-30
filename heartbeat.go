package sjs

// Heartbeat sent from Clients to the Service.
type Heartbeat struct {
	NotifyURL string
	Jobs      []*JobData
}
