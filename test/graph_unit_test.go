package test

import (
	dataflow "prototype/dataflow"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicGraph(t *testing.T) {
	// Schema
	colNames := []string{"Col1", "Col2"}
	schema := &dataflow.Schema{
		ColumnNames: colNames,
	}
	// Create operators
	inputOperator := dataflow.NewInputOperator("table1", schema)
	cids := []uint64{1}
	ops := []dataflow.CompOp{dataflow.LessThan}
	vals := []uint64{15}
	filterOperator := dataflow.NewFilterOperator(cids, ops, vals)
	matviewOperator := dataflow.NewMatViewOperator(0)

	//Create records
	var records []*dataflow.Record
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{1, 10},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{2, 20},
	})

	// Create flow and send records
	graph := dataflow.NewGraph()
	graph.AddInputOperator(inputOperator, true)
	graph.AddNode(filterOperator, inputOperator, true)
	graph.AddOutputOperator(matviewOperator, filterOperator, true)
	graph.Process(-1, -1, "table1", &records)

	assert.Equal(t, len(matviewOperator.Lookup(1)), 1)
	assert.Equal(t, matviewOperator.Lookup(1)[0], records[0])
}
