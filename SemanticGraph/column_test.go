package SemanticGraph

import (
	"fmt"
	"testing"
)

func TestBuildColumn(t *testing.T) {
	col := NewColumn("user_name", 4)
	col2 := BuildColumn(col)
	fmt.Println(col.String(false))
	// Column[user_name , INTEGER , Offset: 0, FixedLength:4]
	col3 := col.Assignment(col2)
	fmt.Println(col3.String(true))
	// user_name : INTEGER
}

func TestTypeSize(t *testing.T) {
	for i := uint8(0); i <= 8; i++ {
		fmt.Print(TypeIdToString(DATA_TYPE(i)))
		fmt.Print(" ")
	}
	//	INVALID BOOLEAN TINYINT SMALLINT INTEGER BIGINT DECIMAL VARCHAR TIMESTAMP
	fmt.Println()
	for i := uint8(1); i <= 8; i++ {
		fmt.Print(TypeSize(DATA_TYPE(i)))
		fmt.Print(" ")
	}
	//	1 1 2 4 8 8 32 8

	/* TypeSize should not be 0. */
}
