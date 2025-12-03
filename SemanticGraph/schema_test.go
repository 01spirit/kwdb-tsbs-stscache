package SemanticGraph

import (
	"fmt"
	"github.com/magiconair/properties/assert"
	"strconv"
	"testing"
)

var columnNames []string = []string{"usage_user", "usage_guest", "usage_nice", "user_name", "arch"}
var dataTypes []uint8 = []uint8{TINYINT, BOOLEAN, INTEGER, DECIMAL, VARCHAR, TIMESTAMP}

func TestBuildSchema(t *testing.T) {
	cols := make([]*Column, 0)
	for i := range columnNames {
		cols = append(cols, NewColumn(columnNames[i], DATA_TYPE(dataTypes[i])))
	}
	schema := NewSchema(cols)
	fmt.Println(schema.String(false))
	// Schema[NumColumns:5, IsInlined:false, Length:46] ::(usage_user : TINYINT, usage_guest : BOOLEAN, usage_nice : INTEGER, user_name : DECIMAL, arch : VARCHAR)
	schema2 := BuildSchema(schema)
	fmt.Println(schema2.String(true))
	// (usage_user : TINYINT, usage_guest : BOOLEAN, usage_nice : INTEGER, user_name : DECIMAL, arch : VARCHAR)

	assert.Equal(t, schema.IsEqual(schema), true)
	assert.Equal(t, schema.IsEqual(schema2), true)

	// return type is uint32, type conversion should be done here.
	for i, cn := range columnNames {
		idx := schema.GetColIdx(cn)
		assert.Equal(t, idx, uint32(i))
	}
	//schema.GetColIdx("panic")
}
func TestSubSchema(t *testing.T) {
	cols1 := make([]*Column, 0)
	cols2 := make([]*Column, 0)
	for i := range columnNames {
		cols1 = append(cols1, NewColumn(columnNames[i], DATA_TYPE(dataTypes[i])))
		suffix := ""
		if i >= 3 {
			suffix = strconv.Itoa(i)
		}
		cols2 = append(cols2, NewColumn(columnNames[i]+suffix, DATA_TYPE(dataTypes[i])))
	}
	sch1 := NewSchema(cols1)
	sch2 := NewSchema(cols2)

	subSchema := SubSchema(sch1, sch2)
	fmt.Println(subSchema.String(false))
	//	Schema[NumColumns:3, IsInlined:true, Length:6] ::(usage_user : TINYINT, usage_guest : BOOLEAN, usage_nice : INTEGER)
}
