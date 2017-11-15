package testhelpers

type ErrorHandler struct {
	LastError error
}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.LastError = err
}
