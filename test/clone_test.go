package test

import (
	dataflow "prototype/dataflow"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGraphClone(t *testing.T) {
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
	// Create flow
	graph := dataflow.NewGraph()
	graph.AddInputOperator(inputOperator, true)
	graph.AddNode(filterOperator, inputOperator, true)
	graph.AddOutputOperator(matviewOperator, filterOperator, true)
	graph.SetIndex(0)
	// Clone flow
	graphClone := graph.Clone(1)
	if _, ok := graphClone.GetNode(0).(*dataflow.InputOperator); !ok {
		t.Errorf("Type mismatch at index 0")
	}
	if _, ok := graphClone.GetNode(1).(*dataflow.FilterOperator); !ok {
		t.Errorf("Type mismatch at index 1")
	}
	if _, ok := graphClone.GetNode(2).(*dataflow.MatViewOperator); !ok {
		t.Errorf("Type mismatch at index 2")
	}

	// Test with some records
	var records []*dataflow.Record
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{1, 10},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{2, 20},
	})

	graph.Process(-1, -1, "table1", &records)
	assert.Equal(t, len(matviewOperator.Lookup(1)), 1)
	assert.Equal(t, matviewOperator.Lookup(1)[0], records[0])

	graphClone.Process(-1, -1, "table1", &records)
	matview1, _ := graphClone.GetNode(2).(*dataflow.MatViewOperator)
	assert.Equal(t, len(matview1.Lookup(1)), 1)
	assert.Equal(t, matview1.Lookup(1)[0], records[0])
}
