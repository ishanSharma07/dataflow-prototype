package dataflow


type Graph struct {
  index uint64
  // Maps node index to node
	nodes map[int]Operator
  // Maps edge index to edge
	edges map[int]*Edge
  inputs map[string]*InputOperator
  outputs []*MatViewOperator
}

func NewGraph() *Graph{
  return &Graph{
    nodes: make(map[int]Operator),
    edges: make(map[int]*Edge),
    inputs: make(map[string]*InputOperator),
  }
}

func (graph *Graph) GetIndex() uint64 {
  return graph.index
}

func (graph *Graph) SetIndex(graphIndex uint64){
  graph.index = graphIndex
}

func (graph *Graph) GetNode(index int) Operator{
  return graph.nodes[index]
}

func (graph *Graph) GetInputs() map[string]*InputOperator{
  return graph.inputs
}

func (graph *Graph) GetOutputs() []*MatViewOperator{
  return graph.outputs
}

func (graph *Graph) MintNodeIndex() int {
	return len(graph.nodes)
}

func (graph *Graph) MintEdgeIndex() int {
	return len(graph.edges)
}

func (graph *Graph) AddNodeMultipleParents(node Operator, parents []Operator, autoIndex bool) {
  var nodeIndex int
  if  autoIndex{
    nodeIndex = graph.MintNodeIndex()
    node.GetCore().SetIndex(nodeIndex)
  } else{
    nodeIndex = node.GetCore().GetIndex()
  }
  node.GetCore().SetGraph(graph)
  graph.nodes[nodeIndex] = node
  for _,parent := range parents{
    graph.AddEdge(parent, node, false)
  }
}

func (graph *Graph) AddNode(node Operator, parent Operator, autoIndex bool){
  parents := []Operator{parent}
  graph.AddNodeMultipleParents(node, parents, autoIndex)
}

// Primarily meant to be used for inserting the exchange operator in an
// existing graph
func (graph *Graph) InsertNode(node Operator, parent Operator){
  // fmt.Printf("[BeforeIns][Graph%d] Num Nodes: %d\n", graph.GetIndex(), len(graph.nodes))
  // // Testing: Print graph status
  // for _, nodE := range graph.nodes{
  //   if _,ok := nodE.(*MatViewOperator); ok{
  //     continue
  //   }
  //   fmt.Printf("[BeforeIns][Graph%d] Parent: %d, Children: %d\n", graph.GetIndex(), nodE.GetCore().GetIndex(), nodE.GetCore().GetChildren()[0].GetCore().GetIndex())
  // }

  node.GetCore().SetGraph(graph)
  nodeIndex := graph.MintNodeIndex()
  node.GetCore().SetIndex(nodeIndex)
  // WARNING: Do not use @parent and @children directly. i.e. modifications made
  // to their internal state will not be reflected in @graph.nodes. This is
  // because the @Operator interface gets copied and not referenced.
  children := parent.GetCore().GetChildren()
  // Delete edges form operators
  // (parent).GetCore().DeleteChildEdges()
  graph.nodes[parent.GetCore().GetIndex()].GetCore().DeleteChildEdges()
  for _, child := range children{
    graph.nodes[child.GetCore().GetIndex()].GetCore().DeleteParentEdge(parent.GetCore().GetIndex())

  }

  // Add @node to the graph, as a consequence chain it with it's \
  // (meant to be) parent
  graph.AddNode(node, graph.nodes[parent.GetCore().GetIndex()], false)
  // Link @node and children
  for _, child := range children{
    graph.AddEdge(node, graph.nodes[child.GetCore().GetIndex()], true)
  }

  // // Testing: Print graph status
  // fmt.Printf("[AfterIns][Graph%d] Num Nodes: %d\n", graph.GetIndex(), len(graph.nodes))
  // for _, nodE := range graph.nodes{
  //   if _,ok := nodE.(*MatViewOperator); ok{
  //     continue
  //   }
  //   fmt.Printf("[AfterIns][Graph%d] Parent: %d, Children: %d\n", graph.GetIndex(), nodE.GetCore().GetIndex(), nodE.GetCore().GetChildren()[0].GetCore().GetIndex())
  // }
}

func (graph *Graph) RemoveEdgeFromNode(){

}

func(graph *Graph) AddInputOperator(node *InputOperator, autoIndex bool){
  graph.inputs[node.GetName()] = node
  parents := []Operator{}
  graph.AddNodeMultipleParents(node, parents, autoIndex)
}

func(graph *Graph) AddOutputOperator(node *MatViewOperator, parent Operator, autoIndex bool){
  graph.outputs = append(graph.outputs, node)
  graph.AddNode(node, parent, autoIndex)
}

// @appendStart is a hack in order to prevent the edges's order getting reversed
// in equijoin. This affects how it interprets left and right sources. In pelton
// we should come up with a better solution.
func (graph *Graph) AddEdge(parent Operator, child Operator, appendStart bool){
  edge := &Edge{
    from: parent,
    to: child,
  }
  // Safety checks
  if edge.From() != graph.nodes[parent.GetCore().GetIndex()]{
    panic("Parent does not match")
  }
  if edge.To() != graph.nodes[child.GetCore().GetIndex()]{
    panic("Child does not match")
  }
  edgeIndex := graph.MintEdgeIndex()
  graph.edges[edgeIndex] = edge
  child.GetCore().AddParent(parent, edge, appendStart)
}

func (graph *Graph) Process(entryIndex int, sourceIndex int, inputName string, records *[]*Record) bool {
  inputNode := graph.inputs[inputName]
  if entryIndex != -1{
    if !graph.nodes[entryIndex].GetCore().ProcessAndForward(sourceIndex, records){
      return false
    }
  } else{
    if !inputNode.GetCore().ProcessAndForward(-1, records){
      return false
    }
  }

  return true
}

func (graph *Graph) Clone(cloneIndex uint64) *Graph{
  clone := NewGraph()
  clone.SetIndex(cloneIndex)
  // Add inputs
  for _, inputOp := range graph.inputs{
    clone.AddInputOperator(inputOp.Clone().(*InputOperator), false)
  }

  // Add nodes by index order
  for i :=0; i < len(graph.nodes); i++{
    op := graph.nodes[i]
    if _, ok := op.(*InputOperator); ok{
      continue
    }
    // Get parents
    var cloneParents []Operator
    for _,parentOp := range op.GetCore().GetParents(){
      opIndex := parentOp.GetCore().GetIndex()
      // The parent node should have already been cloned since we are adding
      // nodes by index order
      cloneParents = append(cloneParents, clone.nodes[opIndex])
    }
    if _, ok:= op.(*MatViewOperator); ok{
      clone.AddOutputOperator(op.Clone().(*MatViewOperator), cloneParents[0], false)
    } else{
      clone.AddNodeMultipleParents(op.Clone(), cloneParents, false)
    }
  }

  // // Testing: Print graph status
  // for _, nodE := range clone.nodes{
  //   if _,ok := nodE.(*MatViewOperator); ok{
  //     continue
  //   }
  //   fmt.Printf("[Clone][Graph%d] Parent: %d, Children: %d\n", clone.GetIndex(), nodE.GetCore().GetIndex(), nodE.GetCore().GetChildren()[0].GetCore().GetIndex())
  // }
  return clone
}

// Supposed to be used as an entry point for a go routine. It invokes Process
// function as and when batches are received
func (graph *Graph) Start(msgChan <-chan *BatchMessage, killChan <-chan bool){
  for{
    select{
    case msg := <- msgChan:
      if msg.EntryIndex !=-1{
        graph.Process(msg.EntryIndex, msg.SourceIndex, "", msg.Records)
      } else{
        if msg.InputName == ""{
          panic("Input name not specified")
        }
        graph.Process(-1, -1, msg.InputName, msg.Records)
      }
    case signal := <- killChan:
      if signal{
        return
      }
    }
  }
}
