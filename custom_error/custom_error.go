package custom_error

import "fmt"

type CustomError struct {
	Action  string
	WrapErr error
}

func UpdateErrorText(action string, err error) error {
	return fmt.Errorf("%s: %w", action, err)
}

func New(action string, err error) *CustomError {
	return &CustomError{
		Action:  action,
		WrapErr: err,
	}
}
