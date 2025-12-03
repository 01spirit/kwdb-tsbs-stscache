package SemanticGraph

import (
	"fmt"
	"github.com/magiconair/properties/assert"
	"testing"
)

//var columnNames []string = []string{"usage_user", "usage_guest", "usage_nice", "user_name", "arch"}
//var dataTypes []uint8 = []uint8{TINYINT, BOOLEAN, INTEGER, DECIMAL, VARCHAR, TIMESTAMP}

func TestBuildPredicate(t *testing.T) {
	preOpType := PREDICATE_OP_TYPE(1)
	col := NewColumn(columnNames[2], DATA_TYPE(dataTypes[2]))
	predicate := NewPredicate(col, preOpType, OpNumValue{1}, "")
	fmt.Println(predicate.String())
	// usage_nice >= 1
	bpre := BuildPredicate(predicate)
	fmt.Println(bpre.String())
	// usage_nice >= 1

	assert.Equal(t, predicate.IsEqual(predicate), true)
	assert.Equal(t, predicate.IsEqual(bpre), true)
}

func TestCompareOpNumValue(t *testing.T) {
	assert.Equal(t, CompareOpNumValue(OpNumValue{1}, OpNumValue{1}, INTEGER), 0)
	assert.Equal(t, CompareOpNumValue(OpNumValue{1}, OpNumValue{2}, INTEGER), -1)
	assert.Equal(t, CompareOpNumValue(OpNumValue{1.2}, OpNumValue{1.11}, DECIMAL), 1)
	assert.Equal(t, CompareOpNumValue(OpNumValue{true}, OpNumValue{false}, BOOLEAN), 1)
	assert.Equal(t, CompareOpNumValue(OpNumValue{"str1"}, OpNumValue{"str1"}, VARCHAR), -1)
	assert.Equal(t, CompareOpNumValue(OpNumValue{true}, OpNumValue{"str"}, BOOLEAN), 1)
	assert.Equal(t, CompareOpNumValue(OpNumValue{1}, OpNumValue{"false"}, BOOLEAN), -1)
}

func TestPredicate_IsContained(t *testing.T) {
	pre1 := NewPredicate(NewColumn("usage_nice", DATA_TYPE(INTEGER)), PREDICATE_OP_TYPE(1), OpNumValue{1}, "") // usage_nice >= 1
	pre2 := NewPredicate(NewColumn("usage_nice", DATA_TYPE(INTEGER)), PREDICATE_OP_TYPE(1), OpNumValue{2}, "") // usage_nice >= 2
	assert.Equal(t, pre1.IsContained(pre2), true)
	assert.Equal(t, pre2.IsContained(pre1), false)
}

func TestBuildPredicates(t *testing.T) {
	pre1 := NewPredicate(NewColumn("usage_nice", DATA_TYPE(INTEGER)), PREDICATE_OP_TYPE(1), OpNumValue{1}, "")     // usage_nice >= 1
	pre2 := NewPredicate(NewColumn("usage_guest", DATA_TYPE(BOOLEAN)), PREDICATE_OP_TYPE(2), OpNumValue{true}, "") // usage_guest = true
	pre3 := NewPredicate(NewColumn("arch", DATA_TYPE(VARCHAR)), PREDICATE_OP_TYPE(2), OpNumValue{}, "x86")         // arch = x86
	predicates := NewPredicates([]*Predicate{pre1, pre2, pre3})
	fmt.Println(predicates.vPres)
	// [usage_nice >= 1 usage_guest = true arch = x86]
	fmt.Println(predicates.index)
	// map[arch:map[2:2] usage_guest:map[2:1] usage_nice:map[1:0]]
	fmt.Println(predicates.presSet)
	// map[usage_nice >= 1:{} usage_guest = true:{} arch = x86:{}]
}

func TestPredicates_IsTotallyContained(t *testing.T) {
	pre1 := NewPredicate(NewColumn("usage_nice", DATA_TYPE(INTEGER)), PREDICATE_OP_TYPE(1), OpNumValue{1}, "")     // usage_nice >= 1
	pre2 := NewPredicate(NewColumn("usage_guest", DATA_TYPE(BOOLEAN)), PREDICATE_OP_TYPE(2), OpNumValue{true}, "") // usage_guest = true
	pre3 := NewPredicate(NewColumn("arch", DATA_TYPE(VARCHAR)), PREDICATE_OP_TYPE(2), OpNumValue{}, "x86")         // arch = x86
	predicates := NewPredicates([]*Predicate{pre1, pre2, pre3})
	predicates2 := NewPredicates([]*Predicate{pre1})
	predicates3 := NewPredicates([]*Predicate{pre2})
	fmt.Println(predicates.IsTotallyContained(predicates2))
	fmt.Println(predicates.IsTotallyContained(predicates3))
	fmt.Println(predicates.IsTotallyContained(predicates))
	fmt.Println(predicates2.IsTotallyContained(predicates))
	fmt.Println(predicates3.IsTotallyContained(predicates))
}
