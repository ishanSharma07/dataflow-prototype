package test

import (
	dataflow "prototype/dataflow"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputBatch(t *testing.T) {
	colNames := []string{"Col1", "Col2"}
	schema := &dataflow.Schema{
		ColumnNames: colNames,
	}
	inputOperator := dataflow.NewInputOperator("table1", schema)
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
	inputOperator.Process(-1, &records, &output)

	assert.Equal(t, output[0], records[0])
	assert.Equal(t, output[1], records[1])
}
