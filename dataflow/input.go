package dataflow

type InputOperator struct {
	Core OperatorCore
	name string
}

func NewInputOperator(name string, schema *Schema) *InputOperator {
	inputOp := &InputOperator{
		name: name,
	}
	inputOpCore := OperatorCore{
		opType:       INPUT,
		opIface:      inputOp,
		InputSchemas: []*Schema{schema},
		OutputSchema: schema,
	}
	inputOp.SetCore(inputOpCore)
	return inputOp
}
func (op *InputOperator) Process(source int, input *[]*Record, output *[]*Record) bool {
	for _, record := range *input {
		*output = append(*output, record)
	}
	return true
}

func (op *InputOperator) GetCore() *OperatorCore {
	return &op.Core
}

func (op *InputOperator) SetCore(core OperatorCore) {
	op.Core = core
}

func (op *InputOperator) ComputeOutputSchema() {
	op.Core.OutputSchema = op.Core.InputSchemas[0]
}

func (op *InputOperator) GetName() string {
	return op.name
}

func (op *InputOperator) Clone() Operator {
	cloneOp := &InputOperator{
		name: op.name,
	}
	cloneOpCore := OperatorCore{
		opType:       INPUT,
		opIface:      cloneOp,
		index:        op.GetCore().GetIndex(),
		InputSchemas: op.GetCore().InputSchemas,
		OutputSchema: op.GetCore().OutputSchema,
	}
	cloneOp.SetCore(cloneOpCore)
	return cloneOp
}
