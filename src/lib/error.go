package error

type EmptyListError struct {
	s string
}

func (e *EmptyListError) Error() string {
	return e.s
}

