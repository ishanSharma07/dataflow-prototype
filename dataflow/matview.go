package dataflow

type MatViewOperator struct {
	Core OperatorCore
	// Resorting to Key of length 1 for the prototype because a  slice cannot be
	// used as a key for map in go since it does not implement equality operarations
	// internally and there is not way to override thos for a custom struct
	state map[uint64][]*Record
	key   uint64
}

func NewMatViewOperator(key uint64) *MatViewOperator {
	matviewOp := &MatViewOperator{
		state: make(map[uint64][]*Record),
		key:   key,
	}
	matviewOpCore := OperatorCore{
		opType:  MATVIEW,
		opIface: matviewOp,
	}
	matviewOp.SetCore(matviewOpCore)
	return matviewOp
}
func (op *MatViewOperator) Process(source int, input *[]*Record, output *[]*Record) bool {
	for _, record := range *input {
		// fmt.Printf("[Graph%d][MATVIEW] Record: %v\n", op.GetCore().GetGraph().GetIndex(), record)
		key := record.GetValue(op.key)
		if _, ok := op.state[key]; ok {
			op.state[key] = append(op.state[key], record)
		} else {
			op.state[key] = []*Record{record}
		}
	}
	return true
}

func (op *MatViewOperator) Lookup(key uint64) []*Record {
	return op.state[key]
}

func (op *MatViewOperator) ComputeOutputSchema() {
	op.Core.OutputSchema = op.Core.InputSchemas[0]
}

func (op *MatViewOperator) GetCore() *OperatorCore {
	return &op.Core
}

func (op *MatViewOperator) SetCore(core OperatorCore) {
	op.Core = core
}

func (op *MatViewOperator) GetKey() uint64 {
	return op.key
}

func (op *MatViewOperator) Clone() Operator {
	cloneOp := &MatViewOperator{
		state: make(map[uint64][]*Record),
		key:   op.key,
	}
	cloneOpCore := OperatorCore{
		opType:  MATVIEW,
		opIface: cloneOp,
		index:   op.GetCore().GetIndex(),
	}
	cloneOp.SetCore(cloneOpCore)
	return cloneOp
}
