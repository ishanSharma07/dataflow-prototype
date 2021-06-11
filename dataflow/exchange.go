package dataflow

type ExchangeOperator struct {
	Core            OperatorCore
	incomingChan    <-chan *BatchMessage
	peerChans       map[uint64]chan *BatchMessage
	graphChan       chan<- *BatchMessage
	partitionColumn uint64
	currentParition uint64
	totalParitions  uint64
}

func NewExchangeOperator(incomingChan <-chan *BatchMessage, graphChan chan<- *BatchMessage, peerChans map[uint64]chan *BatchMessage, paritionColumn uint64, currentParition uint64, totalParitions uint64) *ExchangeOperator {
	exchangeOp := &ExchangeOperator{
		incomingChan:    incomingChan,
		graphChan:       graphChan,
		peerChans:       peerChans,
		partitionColumn: paritionColumn,
		currentParition: currentParition,
		totalParitions:  totalParitions,
	}
	exchangeOpCore := OperatorCore{
		opType:  EXCHANGE,
		opIface: exchangeOp,
	}
	exchangeOp.SetCore(exchangeOpCore)
	go exchangeOp.listenFromPeers()
	return exchangeOp
}

func (op *ExchangeOperator) listenFromPeers() {
	for {
		select {
		case msg := <-op.incomingChan:
			// Set the "entry" node as the child of the current node
			msg.EntryIndex = op.GetCore().Children[0].To().GetCore().GetIndex()
			// Set the source index as the current index
			msg.SourceIndex = op.GetCore().GetIndex()
			op.graphChan <- msg
		}
	}
}

func (op *ExchangeOperator) Process(source int, input *[]*Record, output *[]*Record) bool {
	// If there are no records to process then return
	if len(*input) == 0 {
		return true
	}
	recordsByPartition := op.partitionRecords(input)
	// Forward records that are meant to be in the current parition
	if _, ok := recordsByPartition[op.currentParition]; ok {
		for _, record := range *recordsByPartition[op.currentParition] {
			*output = append(*output, record)
		}
	}

	// delete(recordsByPartition, op.currentParition)
	// Send batches to appropriate peers
	for k := range recordsByPartition {
		if k == op.currentParition {
			continue
		}
		msg := &BatchMessage{
			InputName:  "",
			EntryIndex: -1,
			Records:    recordsByPartition[k],
		}
		op.peerChans[k] <- msg
	}
	return true
}

func (op *ExchangeOperator) partitionRecords(records *[]*Record) map[uint64]*[]*Record {
	// Performs a modulous parition; in pelton we can use a hash based one
	recordsByPartition := make(map[uint64]*[]*Record)
	for _, record := range *records {
		value := record.GetValue(op.partitionColumn)
		partition := value % op.totalParitions
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

func (op *ExchangeOperator) GetCore() *OperatorCore {
	return &op.Core
}

func (op *ExchangeOperator) SetCore(core OperatorCore) {
	op.Core = core
}

func (op *ExchangeOperator) ComputeOutputSchema() {
	op.Core.OutputSchema = op.Core.InputSchemas[0]
}

func (op *ExchangeOperator) Clone() Operator {
	panic("The exchange operator is not meant to be cloned")
}
