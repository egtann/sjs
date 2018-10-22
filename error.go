package sjs

type Missing error

func IsMissing(err error) bool {
	switch errors.Cause(err).(type) {
	case Missing:
		return true
	}
	return false
}
