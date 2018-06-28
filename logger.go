package xdb

type DbLogger struct {
	Logger func(format string, arg ...interface{})
}

func (d DbLogger) Write(p []byte) (n int, err error) {
	if d.Logger != nil {
		d.Logger("DbLogger:%s", string(p))
	}
	return 0, nil
}
