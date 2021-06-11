package test

import (
	dataflow "prototype/dataflow"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterBatch(t *testing.T) {
	cids := []uint64{1}
	ops := []dataflow.CompOp{dataflow.LessThan}
	vals := []uint64{15}
	filterOperator := dataflow.NewFilterOperator(cids, ops, vals)
	colNames := []string{"Col1", "Col2"}
	schema := &dataflow.Schema{
		ColumnNames: colNames,
	}
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
	filterOperator.Process(-1, &records, &output)
	assert.Equal(t, len(output), 1)
	assert.Equal(t, output[0], records[0])
}
