package SemanticGraph

import (
	"fmt"
	"math"
)

type Schema struct {
	length           uint32
	columns          []*Column
	tupleIsInlined   bool
	uninlinedColumns []uint32
}

func NewSchema(columns []*Column) *Schema {
	var curOff uint32 = 0
	tupleIsInlined := true
	unInlinedColumns := make([]uint32, 0)
	newColumns := make([]*Column, 0)
	for i := 0; i < len(columns); i++ {
		col := columns[i]
		if !col.IsInlined() {
			tupleIsInlined = false
			unInlinedColumns = append(unInlinedColumns, uint32(i))
		}
		col.columnOffset = curOff
		curOff += col.fixedLength
		newColumns = append(newColumns, col)
	}
	return &Schema{
		length:           curOff,
		columns:          newColumns,
		tupleIsInlined:   tupleIsInlined,
		uninlinedColumns: unInlinedColumns,
	}
}

func BuildSchema(schema *Schema) *Schema {
	return &Schema{
		length:           schema.length,
		columns:          schema.columns,
		tupleIsInlined:   schema.tupleIsInlined,
		uninlinedColumns: schema.uninlinedColumns,
	}
}

func (s *Schema) Assignment(schema *Schema) *Schema {
	s.columns = schema.columns
	s.length = schema.length
	s.uninlinedColumns = schema.uninlinedColumns
	s.tupleIsInlined = schema.tupleIsInlined
	return s
}

func (s *Schema) IsEqual(schema *Schema) bool {
	if s == schema {
		return true
	}
	if len(s.columns) != len(schema.columns) {
		return false
	}
	for _, col := range s.columns {
		idx := s.TryGetColIdx(col.GetName())
		if idx == math.MaxUint32 {
			return false
		}
		if col != s.columns[idx] {
			return false
		}
	}
	return true
}

func (s *Schema) GetColumns() []*Column {
	return s.columns
}

func (s *Schema) GetColumn(idx uint32) *Column {
	return s.columns[idx]
}

func (s *Schema) GetColIdx(colName string) uint32 {
	idx := s.TryGetColIdx(colName)
	if idx != math.MaxUint32 {
		return idx
	}
	panic("Column does not exist")
}

func (s *Schema) GetUnlinedColumns() []uint32 { return s.uninlinedColumns }

func (s *Schema) GetColumnCount() uint32 { return uint32(len(s.columns)) }

func (s *Schema) GetUnInlinedColumnCount() uint32 { return uint32(len(s.uninlinedColumns)) }

func (s *Schema) GetLength() uint32 { return s.length }

func (s *Schema) IsInlined() bool { return s.tupleIsInlined }

func (s *Schema) TryGetColIdx(colName string) uint32 {
	for i, col := range s.columns {
		if col.GetName() == colName {
			return uint32(i)
		}
	}
	return math.MaxUint32
}

func CopySchema(source *Schema, dest []uint32) *Schema {
	cols := make([]*Column, len(dest))
	for _, d := range dest {
		cols = append(cols, source.columns[d])
	}
	return &Schema{columns: cols}
}

func MergeSchema(s1, s2 *Schema) (*Schema, []uint64) {
	used := make([]uint64, 0)
	columns := make([]*Column, 0)
	tmp := s1.GetColumns()
	columns = append(columns, tmp...)
	size := s2.GetColumnCount()
	for i := 0; i < int(size); i++ {
		col := s2.GetColumn(uint32(i))
		if s1.TryGetColIdx(col.columnName) == math.MaxUint32 {
			columns = append(columns, col)
			used = append(used, uint64(i))
		}
	}
	return NewSchema(columns), used
}

func SubSchema(s1, s2 *Schema) *Schema {
	columns := make([]*Column, 0)
	len2 := s2.GetColumnCount()
	for i := uint32(0); i < len2; i++ {
		col := s2.GetColumn(i)
		if s1.TryGetColIdx(col.columnName) != math.MaxUint32 {
			columns = append(columns, col)
		}
	}
	return NewSchema(columns)
}

func DifferenceSchema(s1, s2 *Schema) *Schema {
	columns := make([]*Column, 0)
	len2 := s2.GetColumnCount()
	for i := uint32(0); i < len2; i++ {
		col := s2.GetColumn(i)
		if s1.TryGetColIdx(col.columnName) == math.MaxUint32 {
			columns = append(columns, col)
		}
	}
	return NewSchema(columns)
}

func GetSubSchemaIdx(input, output *Schema, schemaIndex *[]uint32) {
	*schemaIndex = make([]uint32, output.GetColumnCount())
	for _, col := range output.GetColumns() {
		*schemaIndex = append(*schemaIndex, input.TryGetColIdx(col.columnName))
	}
}

func (s *Schema) String(simplified bool) string {
	if simplified {
		var res string
		first := true
		res += "("
		for i := uint32(0); i < s.GetColumnCount(); i++ {
			if first {
				first = false
			} else {
				res += ", "
			}
			res += s.columns[i].String(simplified)
		}
		res += ")"
		return res
	}

	var res string
	res = fmt.Sprintf("Schema[NumColumns:%d, IsInlined:%v, Length:%d]", s.GetColumnCount(), s.tupleIsInlined, s.length)
	first := true
	res += " ::("
	for i := uint32(0); i < s.GetColumnCount(); i++ {
		if first {
			first = false
		} else {
			res += ", "
		}
		res += s.columns[i].String(true)
	}
	res += ")"
	return res
}
