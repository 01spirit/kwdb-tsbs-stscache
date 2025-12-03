package SemanticGraph

import (
	"fmt"
	"strings"
	"testing"
)

func TestBuildSemanticNode(t *testing.T) {
	cols := make([]*Column, 0)
	for i := range columnNames {
		cols = append(cols, NewColumn(columnNames[i], DATA_TYPE(dataTypes[i])))
	}
	metadata := NewSchema(cols)
	pre1 := NewPredicate(NewColumn("usage_nice", DATA_TYPE(INTEGER)), PREDICATE_OP_TYPE(1), OpNumValue{1}, "")     // usage_nice >= 1
	pre2 := NewPredicate(NewColumn("usage_guest", DATA_TYPE(BOOLEAN)), PREDICATE_OP_TYPE(2), OpNumValue{true}, "") // usage_guest = true
	pre3 := NewPredicate(NewColumn("arch", DATA_TYPE(VARCHAR)), PREDICATE_OP_TYPE(2), OpNumValue{}, "x86")         // arch = x86
	predicates := NewPredicates([]*Predicate{pre1, pre2, pre3})
	node := NewSemanticNode("hash", "metric", metadata, true, predicates,
		Aggregation_Type(NONE), Aggregation_Interval_Type(NATURAL), uint32(NONE))
	node.PrintSemanticNode()

	bnode := BuildSemanticNode(node)
	bnode.PrintSemanticNode()
}

func TestSplitSegment(t *testing.T) {
	input := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#" +
		"{usage_user[int64],usage_system[int64],usage_idle[int64]}#" +
		"{(usage_user>90[int64])(usage_guest>90[int64])}#" +
		"{mean,15m}"
	segments, star, tags := SplitSegment(input)
	fmt.Println(segments)
	//{(cpu.hostname=host_1)}#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{mean,15m}
	//{(cpu.hostname=host_2)}#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{mean,15m}
	//{(cpu.hostname=host_3)}#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{mean,15m}
	fmt.Println(star)
	// {(cpu.*)}#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{mean,15m}
	fmt.Println(tags)
	// host_1
	// host_2
	// host_3
}

func TestExtractBraceContents(t *testing.T) {
	input := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#" +
		"{usage_user[int64],usage_system[int64],usage_idle[int64]}#" +
		"{(usage_user>90[int64])(usage_guest>90[int64])}#" +
		"{mean,15m}"
	result := ExtractBraceContents(input)
	fmt.Println(result)
	// (cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)
	// usage_user[int64],usage_system[int64],usage_idle[int64]
	// (usage_user>90[int64])(usage_guest>90[int64])
	// mean,15m

	predicates := ExtractContentInParentheses(result[2])
	fmt.Println(predicates)
	// usage_user>90[int64]
	// usage_guest>90[int64]

	for _, temp := range predicates {
		start := strings.Index(temp, "[")
		end := strings.Index(temp, "]")
		fieldName := temp[:start]
		fieldType := temp[start+1 : end]
		element := ExtractVariablesSymbolConstant(fieldName)
		fmt.Println(fieldType)
		fmt.Println(element)
		// int64
		// {usage_user > 90}
		// int64
		// {usage_guest > 90}
	}
}

func TestSemanticGraph_SemanticSegmentToNode(t *testing.T) {
	segment := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#" +
		"{usage_user[int64],usage_system[int64],usage_idle[int64]}#" +
		"{(usage_user>90[int64])(usage_guest>90[int64])}#" +
		"{mean,15m}"
	//graph := NewSemanticGraph()
	node := SemanticSegmentToNode(segment)
	node.PrintSemanticNode()
}

func TestSemanticGraph_AddNode(t *testing.T) {
	graph := NewSemanticGraph()
	segments := make([]string, 0)
	for i := 1; i <= 10; i++ {
		segments = append(segments, fmt.Sprintf(
			"{(cpu.hostname=host_%d)(cpu.hostname=host_%d)}#"+
				"{usage_user[int64],usage_system[int64],usage_idle[int64]}#"+
				"{(usage_user>90[int64])(usage_guest>90[int64])}#"+
				"{mean,15m}", i, i*10+1))
	}
	for i := range segments {
		single, star, tags := SplitSegment(segments[i])
		if !graph.IsExistNode(star) {
			node := SemanticSegmentToNode(star)
			graph.AddNode(node)
		}
		for _, s := range single {
			if !graph.IsExistNode(s) {
				snode := SemanticSegmentToNode(s)
				graph.AddNode(snode)
			}
		}
		starNode := graph.FindSemanticNode(star)
		starNode.IncTagVisitTime(tags)
		fmt.Println(starNode.tagVisitTime)
	}
	graph.PrintGraph()
	fmt.Println("node num: ", graph.nodeNum)
	fmt.Println("edge num: ", graph.edgeNum)
}

func TestBuildGraph(t *testing.T) {
	graph := NewSemanticGraph()
	segments := make([]string, 0)

	aggr := []string{"mean", "max", "min"}
	for i := 0; i < 10; i++ {
		segments = append(segments, fmt.Sprintf(
			"{(cpu.hostname=host_%d)(cpu.hostname=host_%d)(cpu.hostname=host_%d)}#"+
				"{usage_user[int64],usage_system[int64],usage_idle[int64]}#"+
				"{(usage_user>90[int64])(usage_guest>90[int64])}#"+
				"{%s,%dm}", i*10, i*10+1, i*10+2, aggr[i%len(aggr)], (i*10)%40))
	}

	for i := range segments {
		single, star, _ := SplitSegment(segments[i])
		if !graph.IsExistNode(star) {
			node := SemanticSegmentToNode(star)
			graph.AddNode(node)
		}
		for _, s := range single {
			if !graph.IsExistNode(s) {
				snode := SemanticSegmentToNode(s)
				graph.AddNode(snode)
			}
		}
	}
	graph.PrintGraph()
	fmt.Println("node num: ", graph.nodeNum)
	fmt.Println("edge num: ", graph.edgeNum)
}
