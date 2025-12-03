package SemanticGraph

import "fmt"

type Column struct {
	columnName     string
	columnType     DATA_TYPE
	fixedLength    uint32
	variableLength uint32
	columnOffset   uint32
}

func TypeSize(dataType DATA_TYPE) uint8 {
	switch uint8(dataType) {
	case BOOLEAN, TINYINT:
		return 1
	case SMALLINT:
		return 2
	case INTEGER:
		return 4
	case BIGINT, DECIMAL, TIMESTAMP:
		return 8
	case VARCHAR:
		return 32
	default:
		panic("Cannot get size of invalid type")
	}
}

func NewColumn(columnName string, datatype DATA_TYPE) *Column {
	var varLen uint32 = 0
	if datatype == DATA_TYPE(VARCHAR) {
		varLen = uint32(TypeSize(datatype))
	}
	return &Column{
		columnName:     columnName,
		columnType:     datatype,
		fixedLength:    uint32(TypeSize(datatype)),
		variableLength: varLen,
		columnOffset:   0,
	}
}

func BuildColumn(col *Column) *Column {
	return &Column{
		columnName:     col.columnName,
		columnType:     col.columnType,
		fixedLength:    col.fixedLength,
		variableLength: col.variableLength,
		columnOffset:   col.columnOffset,
	}
}

func (c *Column) Assignment(col *Column) *Column {
	if c == col {
		return c
	}
	c.columnName = col.columnName
	c.columnType = col.columnType
	c.fixedLength = col.fixedLength
	c.variableLength = col.variableLength
	c.columnOffset = col.columnOffset

	return c
}

func (c *Column) IsEqual(col *Column) bool {
	if c == col {
		return true
	}
	if c.columnName == col.columnName && c.columnType == col.columnType {
		return true
	}
	return false
}

func (c *Column) IsNotEqual(col *Column) bool {
	if c == col {
		return false
	}
	if c.columnName != col.columnName || c.columnType != col.columnType {
		return true
	}
	return false
}

func (c *Column) GetName() string { return c.columnName }

func (c *Column) GetLength() uint32 {
	if c.IsInlined() {
		return c.fixedLength
	}
	return c.variableLength
}

func (c *Column) GetFixedLength() uint32 { return c.fixedLength }

func (c *Column) GetVariableLength() uint32 { return c.variableLength }

func (c *Column) GetOffset() uint32 { return c.columnOffset }

func (c *Column) GetType() DATA_TYPE { return c.columnType }

func (c *Column) IsInlined() bool { return c.columnType != DATA_TYPE(VARCHAR) }

func (c *Column) String(simplified bool) string {
	if simplified {
		return fmt.Sprintf("%s : %s", c.columnName, TypeIdToString(c.columnType))
	}
	var res string
	res = fmt.Sprintf("Column[%s , %s , Offset: %d, ", c.columnName, TypeIdToString(c.columnType), c.columnOffset)
	if c.IsInlined() {
		res += fmt.Sprintf("FixedLength:%d", c.fixedLength)
	} else {
		res += fmt.Sprintf("VarLength:%d", c.variableLength)
	}
	res += "]"
	return res
}
