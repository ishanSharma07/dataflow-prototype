package dataflow

type ProjectOperator struct {
	Core OperatorCore
	cids []uint64
}

func NewProjectOperator(cids []uint64) *ProjectOperator {
	projectOp := &ProjectOperator{
		cids: cids,
	}
	projectOpCore := OperatorCore{
		opType:  PROJECT,
		opIface: projectOp,
	}
	projectOp.SetCore(projectOpCore)
	return projectOp
}

func (op *ProjectOperator) Process(source int, input *[]*Record, output *[]*Record) bool {
	for _, record := range *input {
		outRecord := &Record{
			Data:   record.GetValues(op.cids),
			Schema: op.GetCore().OutputSchema,
		}
		*output = append(*output, outRecord)
	}
	return true
}

func (op *ProjectOperator) GetCore() *OperatorCore {
	return &op.Core
}

func (op *ProjectOperator) SetCore(core OperatorCore) {
	op.Core = core
}

func (op *ProjectOperator) ComputeOutputSchema() {
	var outputColNames []string
	for cid := range op.cids {
		outputColNames = append(outputColNames, op.Core.InputSchemas[0].GetColumnName(uint64(cid)))
	}
	op.GetCore().OutputSchema = &Schema{
		ColumnNames: outputColNames,
	}
}

func (op *ProjectOperator) Clone() Operator {
	cloneOp := &ProjectOperator{
		cids: op.cids,
	}
	cloneOpCore := OperatorCore{
		opType:  PROJECT,
		opIface: cloneOp,
		index:   op.GetCore().GetIndex(),
	}
	cloneOp.SetCore(cloneOpCore)
	return cloneOp
}
