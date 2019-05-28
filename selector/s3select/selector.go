package s3select

import (
	"io"

	"github.com/johannesboyne/gofakes3"
)

type S3Select struct {
}

var _ gofakes3.Selector = &S3Select{}

func (s3s *S3Select) Select(
	from io.Reader,
	expr gofakes3.SelectExpression,
	input gofakes3.SelectInput,
	output gofakes3.SelectOutput,
) (io.ReadCloser, error) {

	return nil, gofakes3.ErrNotImplemented
}
