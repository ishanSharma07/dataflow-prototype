package dataflow

type Record struct {
	// Supports data of type int for now
	Data   []uint64
	Schema *Schema
}

func (this *Record) SetSchema(schema *Schema) {
	this.Schema = schema
}

func (this *Record) GetSchema() *Schema {
	return this.Schema
}

func (this *Record) SetValues(values []uint64) {
	this.Data = values
}

func (this *Record) SetValue(index uint64, value uint64) {
	this.Data[index] = value
}

func (this *Record) GetValue(index uint64) uint64 {
	return this.Data[index]
}

func (this *Record) GetValues(indices []uint64) []uint64 {
	var values []uint64
	for _, index := range indices {
		values = append(values, this.GetValue(index))
	}
	return values
}

func (this *Record) GetAllValues() []uint64 {
	return this.Data
}
