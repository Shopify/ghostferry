package testhelpers

type ErrorHandler struct {
	LastError error
}

func (this *ErrorHandler) ReportError(from string, err error) {
	this.Fatal(from, err)
}

func (this *ErrorHandler) Fatal(from string, err error) {
	this.LastError = err
}
