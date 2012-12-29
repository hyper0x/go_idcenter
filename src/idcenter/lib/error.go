package lib

type EmptyListError struct {
	Msg string
}

func (e EmptyListError) Error() string {
	return e.Msg
}
