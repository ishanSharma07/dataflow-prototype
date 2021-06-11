package test

import (
	"prototype/dataflow"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func makeInputRecords(schema *dataflow.Schema) []*dataflow.Record {
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
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{3, 20},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{4, 10},
	})
	return records
}

func makeSchemasForJoin() (*dataflow.Schema, *dataflow.Schema) {
	colNamesLeft := []string{"Col1", "Col2", "Col3"}
	schemaLeft := &dataflow.Schema{
		ColumnNames: colNamesLeft,
	}
	colNamesRight := []string{"Col4", "Col5"}
	schemaRight := &dataflow.Schema{
		ColumnNames: colNamesRight,
	}
	return schemaLeft, schemaRight
}

func makeLeftRecords(schema *dataflow.Schema) []*dataflow.Record {
	var records []*dataflow.Record
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{1, 10, 5},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{2, 20, 10},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{3, 31, 10},
	})
	return records
}

func makeRightRecords(schema *dataflow.Schema) []*dataflow.Record {
	var records []*dataflow.Record
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{10, 20},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{20, 60},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{30, 60},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{40, 80},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{60, 120},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{31, 62},
	})
	return records
}

func makeThirdInputSchema() *dataflow.Schema {
	colNames := []string{"Col6", "Col7"}
	schema := &dataflow.Schema{
		ColumnNames: colNames,
	}
	return schema
}

func makeThirdInputRecords(schema *dataflow.Schema) []*dataflow.Record {
	var records []*dataflow.Record
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{120, 60},
	})
	records = append(records, &dataflow.Record{
		Schema: schema,
		Data:   []uint64{124, 62},
	})
	return records
}

// DESCRIPTION: A simple graph which has only one partitioning boundary
// (because of matview's key). That is accomplishied via initial partioning of
// records at the input operator, hence no exchange operator is required.
func TestFilterGraph(t *testing.T) {
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

	engine := dataflow.NewDataflowEngine(2, graph)
	engine.StartEngine()

	records := makeInputRecords(schema)
	engine.Process("table1", &records)
	time.Sleep(20 * time.Millisecond)

	// Check outputs
	assert.Equal(t, engine.GetOutput(0).Lookup(4)[0], records[3])
	assert.Equal(t, engine.GetOutput(1).Lookup(1)[0], records[0])
}

// DESCRIPTION: Graph with a single join. Flow gets modified to include an
// exchange operator before the matview since the matview is keyed on a different
// column.
func TestSingleJoinGraph(t *testing.T) {
	leftSchema, rightSchema := makeSchemasForJoin()
	// Create operators
	leftInput := dataflow.NewInputOperator("leftTable", leftSchema)
	rightInput := dataflow.NewInputOperator("rightTable", rightSchema)
	equijoin := dataflow.NewEquiJoinOperator(1, 0)
	matview := dataflow.NewMatViewOperator(0)
	// Create flow
	graph := dataflow.NewGraph()
	graph.AddInputOperator(leftInput, true)
	graph.AddInputOperator(rightInput, true)
	graph.AddNodeMultipleParents(equijoin, []dataflow.Operator{leftInput, rightInput}, true)
	graph.AddOutputOperator(matview, equijoin, true)

	engine := dataflow.NewDataflowEngine(2, graph)
	engine.StartEngine()

	// NOTE: The matview is keyed on Column 0 (refer the operator construction
	// above), hence an exchange operator will partition it on the same.
	leftRecords := makeLeftRecords(leftSchema)
	engine.Process("leftTable", &leftRecords)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(1)), 0)
	assert.Equal(t, len(engine.GetOutput(0).Lookup(2)), 0)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(3)), 0)

	rightRecords := makeRightRecords(rightSchema)
	engine.Process("rightTable", &rightRecords)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(1)), 1)
	assert.Equal(t, len(engine.GetOutput(0).Lookup(2)), 1)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(3)), 1)
	// Check record data (only compare data, since the schema won't be equal
	// due of lack of schema factory)
	assert.Equal(t, engine.GetOutput(1).Lookup(1)[0].Data, []uint64{1, 10, 5, 20})
	assert.Equal(t, engine.GetOutput(0).Lookup(2)[0].Data, []uint64{2, 20, 10, 60})
	assert.Equal(t, engine.GetOutput(1).Lookup(3)[0].Data, []uint64{3, 31, 10, 62})
}

// DESCRIPTION: A graph with 2 joins. Graph gets modified to include two exchange
// operators. One is placed before the matview (since again it is keyed on a
// different column) and the other gets placed before the second join (for one
// of it's inputs)
func TestTwoJoinGraph(t *testing.T) {
	schema1, schema2 := makeSchemasForJoin()
	schema3 := makeThirdInputSchema()
	// Create operators
	input1 := dataflow.NewInputOperator("table1", schema1)
	input2 := dataflow.NewInputOperator("table2", schema2)
	input3 := dataflow.NewInputOperator("table3", schema3)
	join1 := dataflow.NewEquiJoinOperator(1, 0)
	join2 := dataflow.NewEquiJoinOperator(3, 1)
	matview := dataflow.NewMatViewOperator(0)
	// Create flow
	// (Add inputs in a different order so that correctness of graph traversal
	// can be tested. Broadly speaking, the traversal is done in a DFS  manner
	// starting from inputs)
	graph := dataflow.NewGraph()
	graph.AddInputOperator(input3, true)
	graph.AddInputOperator(input1, true)
	graph.AddInputOperator(input2, true)
	graph.AddNodeMultipleParents(join1, []dataflow.Operator{input1, input2}, true)
	graph.AddNodeMultipleParents(join2, []dataflow.Operator{join1, input3}, true)
	graph.AddOutputOperator(matview, join2, true)

	engine := dataflow.NewDataflowEngine(2, graph)
	engine.StartEngine()

	records1 := makeLeftRecords(schema1)
	engine.Process("table1", &records1)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(engine.GetOutput(0).Lookup(2)), 0)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(3)), 0)

	records2 := makeRightRecords(schema2)
	engine.Process("table2", &records2)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(engine.GetOutput(0).Lookup(2)), 0)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(3)), 0)

	records3 := makeThirdInputRecords(schema3)
	engine.Process("table3", &records3)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(engine.GetOutput(0).Lookup(2)), 1)
	assert.Equal(t, len(engine.GetOutput(1).Lookup(3)), 1)

	// Check record data (only compare data, since the schema won't be equal
	// due of lack of schema factory)
	assert.Equal(t, engine.GetOutput(0).Lookup(2)[0].Data, []uint64{2, 20, 10, 60, 120})
	assert.Equal(t, engine.GetOutput(1).Lookup(3)[0].Data, []uint64{3, 31, 10, 62, 124})
}
