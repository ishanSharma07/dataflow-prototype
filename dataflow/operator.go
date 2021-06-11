package dataflow

type Operator interface {
	Process(source int, input *[]*Record, output *[]*Record) bool
	GetCore() *OperatorCore
	ComputeOutputSchema()
	Clone() Operator
}
