package dataflow

type BatchMessage struct {
	// Name of input operator (specify "" if none)
	InputName string
	// "Entry" node for process function (specify -1 if none)
	EntryIndex int
	// The routine is supposed to assume that node identified by @SourceIndex
	// has sent the message. In majority
	// of the cases, this will be the index of the exchange operator, which is the sole parent,
	//  that is receiving the message. However, in case of multiple parents this could be
	// something else. I haven't given much thought to those scenarios, but have
	// included the following in the design rather than hard-coding it.
	SourceIndex int
	// Batch of records to be processed
	Records *[]*Record
}
