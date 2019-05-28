package gofakes3

import "io"

type Selector interface {
	Select(from io.Reader, expr SelectExpression, input SelectInput, output SelectOutput) (io.ReadCloser, error)
}
