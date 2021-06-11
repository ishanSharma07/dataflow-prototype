package dataflow

import "fmt"

type EquiJoinOperator struct {
	Core       OperatorCore
	leftID     uint64
	rightID    uint64
	leftTable  map[uint64][]*Record
	rightTable map[uint64][]*Record
}

func NewEquiJoinOperator(leftID uint64, rightID uint64) *EquiJoinOperator {
	equijoinOp := &EquiJoinOperator{
		leftID:     leftID,
		rightID:    rightID,
		leftTable:  make(map[uint64][]*Record),
		rightTable: make(map[uint64][]*Record),
	}
	equijoinOpCore := OperatorCore{
		opType:  EQUIJOIN,
		opIface: equijoinOp,
	}
	equijoinOp.SetCore(equijoinOpCore)
	return equijoinOp
}

func (op *EquiJoinOperator) Process(source int, input *[]*Record, output *[]*Record) bool {
	for _, record := range *input {
		if source == op.leftIndex() {
			// fmt.Printf("[EQUI] Node: %d, Source: %d, leftIndex: %d, rightIndex: %d, Record: %v\n", op.GetCore().GetIndex(), source, op.leftIndex(), op.rightIndex(), *record)
			leftValue := record.GetValue(op.leftID)
			// Match with all seen records in right table
			for _, rightRecord := range op.rightTable[leftValue] {
				op.emitRecord(record, rightRecord, output)
			}
			// Store record in left table
			op.leftTable[leftValue] = append(op.leftTable[leftValue], record)
		} else if source == op.rightIndex() {
			rightValue := record.GetValue(op.rightID)
			// Match with all seen records in left table
			for _, leftRecord := range op.leftTable[rightValue] {
				op.emitRecord(leftRecord, record, output)
			}
			// Store record in right table
			op.rightTable[rightValue] = append(op.rightTable[rightValue], record)
		} else {
			fmt.Printf("[EQUI] Node: %d, Source: %d, leftIndex: %d, rightIndex: %d, Record: %v\n", op.GetCore().GetIndex(), source, op.leftIndex(), op.rightIndex(), *record)
			panic("Invalid source in equijoin")
		}
	}

	return true
}

func (op *EquiJoinOperator) emitRecord(left *Record, right *Record, output *[]*Record) {
	// Join left and right records; do not include rightID
	var outRecordData []uint64
	outRecordData = append(outRecordData, left.GetAllValues()...)
	for i := range right.GetAllValues() {
		if uint64(i) == op.rightID {
			continue
		}
		outRecordData = append(outRecordData, right.GetValue(uint64(i)))
	}
	outRecord := &Record{
		Data:   outRecordData,
		Schema: op.Core.OutputSchema,
	}
	// fmt.Printf("[Graph%d][Join: %d] Emitting: %v\n", op.GetCore().GetGraph().GetIndex(), op.GetCore().GetIndex(), outRecord)
	*output = append(*output, outRecord)
}

func (op *EquiJoinOperator) leftIndex() int {
	return op.GetCore().Parents[0].From().GetCore().GetIndex()
}

func (op *EquiJoinOperator) rightIndex() int {
	return op.GetCore().Parents[1].From().GetCore().GetIndex()
}

func (op *EquiJoinOperator) GetCore() *OperatorCore {
	return &op.Core
}

func (op *EquiJoinOperator) SetCore(core OperatorCore) {
	op.Core = core
}

func (op *EquiJoinOperator) GetParitionColumn() uint64 {
	// based on the current implementation of schema of output records it is
	// the leftID
	return op.leftID
}

func (op *EquiJoinOperator) GetLeftPartitionColumn() uint64 {
	return op.leftID
}

func (op *EquiJoinOperator) GetRightPartitionColumn() uint64 {
	return op.rightID
}

func (op *EquiJoinOperator) ComputeOutputSchema() {
	var outputColNames []string
	outputColNames = append(outputColNames, op.GetCore().Parents[0].From().GetCore().OutputSchema.ColumnNames...)
	for i := range op.GetCore().Parents[1].From().GetCore().OutputSchema.ColumnNames {
		if uint64(i) == op.leftID {
			continue
		}
		outputColNames = append(outputColNames, op.GetCore().Parents[1].From().GetCore().OutputSchema.ColumnNames[i])
	}
	op.GetCore().OutputSchema = &Schema{
		ColumnNames: outputColNames,
	}
}

func (op *EquiJoinOperator) Clone() Operator {
	cloneOp := &EquiJoinOperator{
		leftID:     op.leftID,
		rightID:    op.rightID,
		leftTable:  make(map[uint64][]*Record),
		rightTable: make(map[uint64][]*Record),
	}
	cloneOpCore := OperatorCore{
		opType:  EQUIJOIN,
		opIface: cloneOp,
		index:   op.GetCore().GetIndex(),
	}
	cloneOp.SetCore(cloneOpCore)
	return cloneOp
}
