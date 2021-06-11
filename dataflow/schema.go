package dataflow

type Schema struct {
	ColumnNames []string
}

func (this *Schema) SetColumnNames(names []string) {
	this.ColumnNames = names
}

func (this *Schema) GetColumnName(index uint64) string {
	return this.ColumnNames[index]
}
