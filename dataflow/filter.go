package dataflow

type CompOp uint8

const (
	LessThan CompOp = iota
	GreaterThan
	Equal
)

type FilterOperator struct {
	Core OperatorCore
	cids []uint64
	ops  []CompOp
	vals []uint64
}

func NewFilterOperator(cids []uint64, ops []CompOp, vals []uint64) *FilterOperator {
	filterOp := &FilterOperator{
		cids: cids,
		ops:  ops,
		vals: vals,
	}
	filterOpCore := OperatorCore{
		opType:  FILTER,
		opIface: filterOp,
	}
	filterOp.SetCore(filterOpCore)
	return filterOp
}

func evaluate(record *Record, cid uint64, operator CompOp, value uint64) bool {
	switch operator {
	case LessThan:
		if record.GetValue(cid) < value {
			return true
		}
		break
	case GreaterThan:
		if record.GetValue(cid) > value {
			return true
		}
		break
	case Equal:
		if record.GetValue(cid) == value {
			return true
		}
		break
	default:
		panic("Invalid filter condition")
	}
	return false
}

func (op *FilterOperator) accept(record *Record) bool {
	// Implicitly performs logical AND on the filter operations
	for i, _ := range op.cids {
		if !evaluate(record, op.cids[i], op.ops[i], op.vals[i]) {
			return false
		}
	}
	return true
}

func (op *FilterOperator) Process(source int, input *[]*Record, output *[]*Record) bool {
	if len(op.cids) != len(op.ops) || len(op.ops) != len(op.vals) {
		panic("Invalid filter operations")
	}
	for _, record := range *input {
		if !op.accept(record) {
			continue
		}
		*output = append(*output, record)
	}
	return true
}

func (op *FilterOperator) GetCore() *OperatorCore {
	return &op.Core
}

func (op *FilterOperator) SetCore(core OperatorCore) {
	op.Core = core
}

func (op *FilterOperator) ComputeOutputSchema() {
	op.Core.OutputSchema = op.Core.InputSchemas[0]
}

func (op *FilterOperator) Clone() Operator {
	cloneOp := &FilterOperator{
		cids: op.cids,
		ops:  op.ops,
		vals: op.vals,
	}
	cloneOpCore := OperatorCore{
		opType:  FILTER,
		opIface: cloneOp,
		index:   op.GetCore().GetIndex(),
	}
	cloneOp.SetCore(cloneOpCore)
	return cloneOp
}
