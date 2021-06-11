package test

import (
	dataflow "prototype/dataflow"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEquiJoinBatch(t *testing.T) {
	colNamesLeft := []string{"Col1", "Col2", "Col3"}
	schemaLeft := &dataflow.Schema{
		ColumnNames: colNamesLeft,
	}
	colNamesRight := []string{"Col4", "Col5"}
	schemaRight := &dataflow.Schema{
		ColumnNames: colNamesRight,
	}
	var leftRecords []*dataflow.Record
	leftRecords = append(leftRecords, &dataflow.Record{
		Schema: schemaLeft,
		Data:   []uint64{1, 10, 5},
	})
	leftRecords = append(leftRecords, &dataflow.Record{
		Schema: schemaLeft,
		Data:   []uint64{2, 20, 10},
	})
	var rightRecords []*dataflow.Record
	rightRecords = append(rightRecords, &dataflow.Record{
		Schema: schemaRight,
		Data:   []uint64{10, 20},
	})
	rightRecords = append(rightRecords, &dataflow.Record{
		Schema: schemaRight,
		Data:   []uint64{30, 60},
	})

	equijoinOperator := dataflow.NewEquiJoinOperator(1, 0)
	leftInputOperator := dataflow.NewInputOperator("left", schemaLeft)
	rightInputOperator := dataflow.NewInputOperator("right", schemaRight)
	leftInputOperator.GetCore().SetIndex(0)
	rightInputOperator.GetCore().SetIndex(1)
	equijoinOperator.GetCore().Parents = []*dataflow.Edge{
		dataflow.NewEdge(leftInputOperator, equijoinOperator),
		dataflow.NewEdge(rightInputOperator, equijoinOperator),
	}

	var output []*dataflow.Record
	equijoinOperator.Process(0, &leftRecords, &output)
	equijoinOperator.Process(1, &rightRecords, &output)

	assert.Equal(t, len(output), 1)
	// Only compare data, since the schema won't be equal due of lack of schema factory
	assert.Equal(t, output[0].Data, []uint64{1, 10, 5, 20})
}
