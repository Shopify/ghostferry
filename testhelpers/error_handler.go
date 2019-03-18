package testhelpers

type ErrorHandler struct {
	LastError error
}

func (this *ErrorHandler) ReportError(from string, err error) bool {
	this.Fatal(from, err)
	return true
}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.LastError = err
}
