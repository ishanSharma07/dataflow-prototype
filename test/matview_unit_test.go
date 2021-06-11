package test

import (
	dataflow "prototype/dataflow"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatviewBatch(t *testing.T) {
	colNames := []string{"Col1", "Col2"}
	schema := &dataflow.Schema{
		ColumnNames: colNames,
	}
	matviewOperator := dataflow.NewMatViewOperator(0)
	var records []*dataflow.Record
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{1, 10},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{2, 20},
	})

	var output []*dataflow.Record
	matviewOperator.Process(-1, &records, &output)

	assert.Equal(t, matviewOperator.Lookup(1)[0], records[0])
	assert.Equal(t, matviewOperator.Lookup(2)[0], records[1])
}
