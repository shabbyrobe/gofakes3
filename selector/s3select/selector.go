package s3select

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/davecgh/go-spew/spew"
	"github.com/johannesboyne/gofakes3"
)

type Selector struct{}

var _ gofakes3.Selector = &Selector{}

func NewSelector() *Selector {
	return &Selector{}
}

// Useful links:
//
// - https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
// - https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html#s3-glacier-select-sql-reference-from
//
func (selector *Selector) Select(
	from io.Reader,
	expr gofakes3.SelectExpression,
	input gofakes3.SelectInput,
	output gofakes3.SelectOutput,
) (io.ReadCloser, error) {

	switch input.(type) {
	case *gofakes3.SelectJSONInput:
		return selector.selectJSON(from, expr, input, output)
	case *gofakes3.SelectCSVInput:
		return nil, gofakes3.ErrNotImplemented
	case *gofakes3.SelectParquetInput:
		return nil, gofakes3.ErrNotImplemented
	default:
		return nil, gofakes3.ErrInvalidDataSource
	}
}

func (selector *Selector) selectJSON(
	from io.Reader,
	expr gofakes3.SelectExpression,
	input gofakes3.SelectInput,
	output gofakes3.SelectOutput,
) (io.ReadCloser, error) {

	query, err := parseQuery(expr)
	if err != nil {
		return nil, err
	}

	spew.Dump(query)

	jd := json.NewDecoder(from)

	for {
		into := make(map[string]interface{})
		if err := jd.Decode(&into); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// var cur interface{}
		for _, fromPart := range query.FromPath {
			switch fromPart.Kind {
			case PathProp:
			case PathArray:
			case PathWildcard:
			default:
				return nil, gofakes3.ErrInternal // FIXME
			}
		}
	}

	// p.PrintSyntaxTree()

	return nil, gofakes3.ErrNotImplemented
}

func parseQuery(expr gofakes3.SelectExpression) (*SelectQuery, error) {
	if expr.ExpressionType != gofakes3.SelectExpressionSQL {
		return nil, gofakes3.ErrNotImplemented
	}

	query := &SelectQuery{}

	p := &Peg{SelectQuery: query, Buffer: expr.Expression}
	if err := p.Init(); err != nil {
		return nil, gofakes3.ErrInternal
	}

	if err := p.Parse(); err != nil {
		fmt.Println(err)
		// FIXME: until we can report more specific errors about the nature of the
		// SQL parsing failure (the select error codes are quite detailed and
		// specific), this will do.
		return nil, gofakes3.ErrUnsupportedSyntax
	}

	p.Execute()

	return query, nil
}
