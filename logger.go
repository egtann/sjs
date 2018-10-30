package sjs

type Logger interface {
	Printf(s string, args ...interface{})
}

type OptLogger struct{ Log Logger }

func (o *OptLogger) Printf(s string, args ...interface{}) {
	if o.Log == nil {
		return
	}
	o.Log.Printf(s, args...)
}
