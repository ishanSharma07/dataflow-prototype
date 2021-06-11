package dataflow

type OperatorType uint8

const (
	FILTER OperatorType = iota
	INPUT
	MATVIEW
	PROJECT
	EQUIJOIN
	EXCHANGE
)

// This struct is used in the form of type composition since golang does not
// support extending/inheriting structs
type OperatorCore struct {
	opType       OperatorType
	index        int
	opIface      Operator
	InputSchemas []*Schema
	OutputSchema *Schema
	Children     []*Edge
	Parents      []*Edge
	graph        *Graph
	// Used by the engine for graph traversal
	IsVisited bool
}

func (this *OperatorCore) AddParent(parent Operator, edge *Edge, appendStart bool) {
	// Safety check
	if edge.To().GetCore() != this {
		panic("Safety check failed when adding EDGE")
	}
	if edge.From().GetCore() != parent.GetCore() {
		panic("Safety check failed when adding EDGE")
	}

	parent.ComputeOutputSchema()
	if appendStart {
		this.Parents = append([]*Edge{edge}, this.Parents...)
	} else {
		this.Parents = append(this.Parents, edge)
	}
	this.InputSchemas = append(this.InputSchemas, parent.GetCore().OutputSchema)
	parent.GetCore().Children = append(parent.GetCore().Children, edge)
	// edge.To().ComputeOutputSchema()
}

func (this *OperatorCore) ProcessAndForward(sourceIndex int, records *[]*Record) bool {
	var output []*Record
	// fmt.Printf("[Proc][Graph%d] Index: %d; Type: %d; #records: %d\n", this.GetGraph().GetIndex(), this.GetIndex(), this.opType, len(*records))
	if !this.opIface.Process(sourceIndex, records, &output) {
		return false
	}
	for _, edge := range this.Children {
		child := edge.To()
		if !child.GetCore().ProcessAndForward(this.GetIndex(), &output) {
			return false
		}
	}
	return true
}

func (this *OperatorCore) GetParents() []Operator {
	var parentOps []Operator
	for _, parentEdge := range this.Parents {
		parentOps = append(parentOps, parentEdge.From())
	}
	return parentOps
}

func (this *OperatorCore) GetChildren() []Operator {
	var childrenOps []Operator
	for _, childEdge := range this.Children {
		childrenOps = append(childrenOps, childEdge.To())
	}
	return childrenOps
}

func (this *OperatorCore) DeleteChildEdges() {
	this.Children = nil
}

func (this *OperatorCore) DeleteParentEdge(fromIndex int) {
	index := this.getEdgeIndex(fromIndex, this.Parents)
	this.Parents = append(this.Parents[:index], this.Parents[index+1:]...)
}

func (this *OperatorCore) getEdgeIndex(fromIndex int, edges []*Edge) int {
	for i, e := range edges {
		if e.From().GetCore().GetIndex() == fromIndex {
			if e.To().GetCore().GetIndex() != this.GetIndex() {
				panic("Safety check failed")
			}
			return i
		}
	}
	return -1
}

func (this *OperatorCore) SetGraph(graph *Graph) {
	this.graph = graph
}

func (this *OperatorCore) GetGraph() *Graph {
	return this.graph
}

func (this *OperatorCore) SetIndex(idx int) {
	this.index = idx
}

func (this *OperatorCore) GetIndex() int {
	return this.index
}
