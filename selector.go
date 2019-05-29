package gofakes3

import "io"

type Selector interface {
	// 'from' will always contain an uncompressed reader.
	Select(
		from io.Reader,
		expr SelectExpression,
		input SelectInput,
		output SelectOutput,
	) (io.ReadCloser, error)
}
