package dataflow

import "fmt"

type DataflowEngine struct {
	baseGraph      *Graph
	partitionCount uint64
	graphs         map[uint64]*Graph
	graphChans     map[uint64]chan *BatchMessage
	killChans      map[uint64]chan bool
	inputPartition map[string]uint64
}

func NewDataflowEngine(partitionCount uint64, graph *Graph) *DataflowEngine {
	return &DataflowEngine{
		baseGraph:      graph,
		partitionCount: partitionCount,
		graphs:         make(map[uint64]*Graph),
		graphChans:     make(map[uint64]chan *BatchMessage),
		killChans:      make(map[uint64]chan bool),
		inputPartition: make(map[string]uint64),
	}
}

func (engine *DataflowEngine) StartEngine() {
	// Clone and establish channels for communicating with graphs
	var i uint64
	for i = 0; i < engine.partitionCount; i++ {
		engine.graphs[i] = engine.baseGraph.Clone(i)
		engine.graphChans[i] = make(chan *BatchMessage)
		engine.killChans[i] = make(chan bool)
		fmt.Printf("[ENGINE] Cloned: Graph%d\n", i)
	}
	engine.traverseBaseGraph()
	fmt.Printf("[ENGINE] Traversal of base graph complete.\n")
	fmt.Printf("[ENGINE] Input Operators to be partitioned by: %v\n", engine.inputPartition)
	// Launch goroutines
	for k := range engine.graphs {
		go engine.graphs[k].Start(engine.graphChans[k], engine.killChans[k])
	}
	fmt.Printf("[ENGINE] Launched graphs in go routines.\n")
}

func (engine *DataflowEngine) Process(inputName string, records *[]*Record) {
	var recordsByPartition map[uint64]*[]*Record
	if partitionColumn, ok := engine.inputPartition[inputName]; ok {
		recordsByPartition = engine.partitionRecords(records, partitionColumn)
	} else {
		// By default partition by 0th column; in pelton this would translate to
		// partitioning by record's key
		recordsByPartition = engine.partitionRecords(records, 0)
	}
	// Send records to appropriate partitions
	for k := range recordsByPartition {
		fmt.Printf("[ENGINE] Sending batch of %d record(s) to partition %d\n", len(*recordsByPartition[k]), k)
		engine.graphChans[k] <- &BatchMessage{
			InputName:  inputName,
			EntryIndex: -1,
			Records:    recordsByPartition[k],
		}
	}
}

func (engine *DataflowEngine) GetOutput(partition uint64) *MatViewOperator {
	return engine.graphs[partition].GetOutputs()[0]
}

func (engine *DataflowEngine) partitionRecords(records *[]*Record, partitionColumn uint64) map[uint64]*[]*Record {
	// Performs a modulous parition; in pelton we can use a hash based one
	recordsByPartition := make(map[uint64]*[]*Record)
	for _, record := range *records {
		value := record.GetValue(partitionColumn)
		partition := value % engine.partitionCount
		if _, ok := recordsByPartition[partition]; ok {
			*recordsByPartition[partition] = append(*recordsByPartition[partition], record)
		} else {
			tempRecords := make([]*Record, 1)
			recordsByPartition[partition] = &tempRecords
			(*recordsByPartition[partition])[0] = record
		}
	}
	return recordsByPartition
}

func (engine *DataflowEngine) traverseBaseGraph() {
	for _, op := range engine.baseGraph.GetInputs() {
		if op.GetCore().IsVisited {
			fmt.Printf("Visited Node: %d of type %d", op.GetCore().GetIndex(), op.GetCore().GetIndex())
			continue
		}
		engine.visitNode(op)
	}
}

func (engine *DataflowEngine) visitNode(node Operator) {
	if node.GetCore().IsVisited {
		return
	}
	fmt.Printf("[VISIT] Node: %d of Type: %d\n", node.GetCore().GetIndex(), node.GetCore().opType)
	node.GetCore().IsVisited = true
	switch node.(type) {
	case *InputOperator:
		// The initial partitioning column will be decided later (based on join
		// matview... etc). An input operator by default is partitioned by 0th column.
		// (semantically could be partitioned by the record key as well)
		engine.visitNode(node.GetCore().GetChildren()[0])
	case *FilterOperator:
		engine.visitNode(node.GetCore().GetChildren()[0])
	case *ProjectOperator:
		// These operators don't require any shuffle
		engine.visitNode(node.GetCore().GetChildren()[0])
		return
	case *MatViewOperator:
		isPartitioned, _ := engine.getRecentPartition(node, false)
		if !isPartitioned {
			// Simply employ paritioning at the input; no exchange operator is needed
			for _, inputOp := range engine.baseGraph.GetInputs() {
				// There could be multiple inputs because of a union, nevertheless they
				// all will be partitioned by the same column
				engine.inputPartition[inputOp.GetName()] = node.(*MatViewOperator).GetKey()
			}
		} else {
			// Needs an exchange operator
			engine.addExchangeAfter(node.GetCore().GetParents()[0], node.(*MatViewOperator).GetKey())
		}
		return
	case *EquiJoinOperator:
		fmt.Printf("[VISIT] EquiJoin\n")
		leftOp := node.(*EquiJoinOperator).GetCore().GetParents()[0]
		rightOp := node.(*EquiJoinOperator).GetCore().GetParents()[1]
		isLeftPartitioned, _ := engine.getRecentPartition(leftOp, true)
		isRightPartitioned, _ := engine.getRecentPartition(rightOp, true)
		if !isLeftPartitioned && !isRightPartitioned {
			// Records to be partitioned at the relevant input operators; no exchange
			// operator is needed
			leftSubgraphInputs := engine.getSubgraphInputs(leftOp)
			rightSubgraphInputs := engine.getSubgraphInputs(rightOp)
			// All left inputs must be partitioned by the same column
			for _, leftInput := range leftSubgraphInputs {
				engine.inputPartition[leftInput.GetName()] = node.(*EquiJoinOperator).GetLeftPartitionColumn()
			}
			// All right inputs must be partitioned by the same column
			for _, rightInput := range rightSubgraphInputs {
				engine.inputPartition[rightInput.GetName()] = node.(*EquiJoinOperator).GetRightPartitionColumn()
			}
		} else if !isLeftPartitioned {
			// For left: partition at input, for right: add exchange operator
			leftInputs := engine.getSubgraphInputs(leftOp)
			for _, inputOp := range leftInputs {
				engine.inputPartition[inputOp.GetName()] = node.(*EquiJoinOperator).GetLeftPartitionColumn()
			}
			engine.addExchangeAfter(rightOp, node.(*EquiJoinOperator).GetRightPartitionColumn())
		} else if !isRightPartitioned {
			// For right: partition at input, for left: add exchange operator
			rightInputs := engine.getSubgraphInputs(rightOp)
			for _, inputOp := range rightInputs {
				engine.inputPartition[inputOp.GetName()] = node.(*EquiJoinOperator).GetRightPartitionColumn()
			}
			engine.addExchangeAfter(leftOp, node.(*EquiJoinOperator).GetLeftPartitionColumn())
		} else {
			// Needs exchange for both parents
			engine.addExchangeAfter(rightOp, node.(*EquiJoinOperator).GetRightPartitionColumn())
			engine.addExchangeAfter(leftOp, node.(*EquiJoinOperator).GetLeftPartitionColumn())
		}
		engine.visitNode(node.GetCore().GetChildren()[0])
		return
	default:
		fmt.Printf("Type: %d\n", node.GetCore().opType)
		panic("Unsupported operator")
	}
}

func (engine *DataflowEngine) getRecentPartition(node Operator, checkSelf bool) (bool, uint64) {
	if checkSelf {
		if _, ok := node.(*InputOperator); ok {
			if _, ok := engine.inputPartition[node.(*InputOperator).GetName()]; ok {
				return true, engine.inputPartition[node.(*InputOperator).GetName()]
			}
			return false, 0
		}
		// Else, check recursively until a partition boundary is encountered.
		// Reuse this existing method; start checking from one level lower (since
		// @node has to also be checked)
		return engine.getRecentPartition(node.GetCore().GetChildren()[0], false)
	}
	// Possibility of multiple parents here?
	// ->There will be the case with union op as a consequence of
	// fork join (which is currently not supported)
	parent := node.GetCore().GetParents()[0]
	switch parent.(type) {
	case *FilterOperator:
	case *ProjectOperator:
		return engine.getRecentPartition(parent, false)
	case *InputOperator:
		if _, ok := engine.inputPartition[parent.(*InputOperator).GetName()]; ok {
			return true, engine.inputPartition[parent.(*InputOperator).GetName()]
		}
		return false, 0
	case *EquiJoinOperator:
		// Equijoin will always emit records partitioned by the joined column.
		return true, parent.(*EquiJoinOperator).GetParitionColumn()
	default:
		panic("Unexpected operator encounterd when obtaining recent partition column")
	}
	return false, 0
}

// Get input operators for the subgraph that starts by @node
func (engine *DataflowEngine) getSubgraphInputs(node Operator) []*InputOperator {
	// Check if @node is an input operator
	if _, ok := node.(*InputOperator); ok {
		return []*InputOperator{node.(*InputOperator)}
	}

	parents := node.GetCore().GetParents()
	inputs := make([]*InputOperator, 0)
	for _, parent := range parents {
		if _, ok := parent.(*InputOperator); ok {
			inputs = append(inputs, parent.(*InputOperator))
		} else {
			inputs = append(inputs, engine.getSubgraphInputs(parent)...)
		}
	}
	return inputs
}

func (engine *DataflowEngine) addExchangeAfter(node Operator, partitionColumn uint64) {
	fmt.Printf("[ENGINE] Inserting exchange after Node: %d\n", node.GetCore().GetIndex())
	// Initialise comm channels
	exchangeChans := make(map[uint64]chan *BatchMessage)
	var i uint64
	for i = 0; i < engine.partitionCount; i++ {
		exchangeChans[i] = make(chan *BatchMessage)
	}
	// Initialise exchange ops
	exchangeOps := make(map[uint64]Operator)
	for i = 0; i < engine.partitionCount; i++ {
		exchangeOps[i] = NewExchangeOperator(exchangeChans[i], engine.graphChans[i], exchangeChans, partitionColumn, i, engine.partitionCount)
	}
	// Insert exchage operators in their respective graphs
	for i = 0; i < engine.partitionCount; i++ {
		engine.graphs[i].InsertNode(exchangeOps[i], node)
	}
	return
}
