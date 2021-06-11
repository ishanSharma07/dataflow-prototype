package dataflow

type Edge struct {
	from Operator
	to   Operator
}

func NewEdge(from Operator, to Operator) *Edge {
	return &Edge{
		from: from,
		to:   to,
	}
}
func (this *Edge) From() Operator {
	return this.from
}

func (this *Edge) SetFrom(node Operator) {
	this.from = node
}

func (this *Edge) To() Operator {
	return this.to
}

func (this *Edge) SetTo(node Operator) {
	this.to = node
}
