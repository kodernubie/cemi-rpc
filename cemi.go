package cemirpc

type ServeHandler func(methodName string, params ...any) ([]any, error)

type Result struct {
	Error error
	Data  []any
}

type DTO struct {
	Error string
	Data  []any
}

func Serve(methodName string, handler ServeHandler, workerNo ...int) error {

	return nil
}

func Call(methodName string, params ...any) Result {

	ret := Result{}

	return ret
}
