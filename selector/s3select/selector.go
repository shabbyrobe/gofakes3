package s3select

import (
	"io"

	"github.com/johannesboyne/gofakes3"
)

type Selector struct {
}

var _ gofakes3.Selector = &Selector{}

func NewSelector() *Selector {
	return &Selector{}
}

func (selector *Selector) Select(
	from io.Reader,
	expr gofakes3.SelectExpression,
	input gofakes3.SelectInput,
	output gofakes3.SelectOutput,
) (io.ReadCloser, error) {

	if expr.ExpressionType != gofakes3.SelectExpressionSQL {
		return nil, gofakes3.ErrNotImplemented
	}

	// stmt, err := sqlparser.Parse(expr.Expression)
	// if err != nil {
	//     // FIXME: until we can report more specific errors about the nature of the
	//     // SQL parsing failure (the select error codes are quite detailed and
	//     // specific), this will do.
	//     return nil, gofakes3.ErrUnsupportedSyntax
	// }
	// _ = stmt
	//
	// spew.Dump(stmt)

	return nil, gofakes3.ErrNotImplemented
}
