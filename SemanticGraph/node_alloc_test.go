package SemanticGraph

import (
	"fmt"
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestCopyNum(t *testing.T) {
	segment1 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"
	segment2 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{max,15m}"
	segment3 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{min,15m}"
	segment4 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"
	segment5 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}"
	segment6 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{empty,empty}"
	segment7 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_idle[int64],usage_nice[int64]}#{empty}#{empty,empty}"
	segment8 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[int64],usage_nice[int64]}#{empty}#{empty,empty}"

	hr := HistoryRecord{Record: map[string]uint64{segment1: 150, segment2: 250, segment3: 350, segment4: 450, segment5: 550, segment6: 650, segment7: 750, segment8: 850}}
	qs := hr.StatStarSegment()
	g := BuildSemanticGraph(qs)
	avgLoad := qs.AvgLoad()
	CopyNum(qs, g, avgLoad)
	for _, node := range g.hashTable {
		fmt.Println(node.hashCode, node.copyNum)
	}
}

func TestTraverseSort(t *testing.T) {
	segment1 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"
	segment2 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{max,15m}"
	segment3 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{min,15m}"
	segment4 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"
	segment5 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}"
	segment6 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{empty,empty}"
	segment7 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_idle[int64],usage_nice[int64]}#{empty}#{empty,empty}"
	segment8 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[int64],usage_nice[int64]}#{empty}#{empty,empty}"

	hr := HistoryRecord{Record: map[string]uint64{segment1: 150, segment2: 250, segment3: 350, segment4: 450, segment5: 550, segment6: 650, segment7: 750, segment8: 850}}
	qs := hr.StatStarSegment()
	g := BuildSemanticGraph(qs)

	names := TraverseSort(g)
	for _, node := range names {
		fmt.Println(node.level)
	}
}

func TestSelectInstance(t *testing.T) {
	hr := NewHistoryRecord()
	segments := GenerateCpuSegment()
	for i, s := range segments {
		hr.Record["{(cpu.hostname=host_1)(cpu.hostname=host_2)}#"+s] = uint64(i)
	}
	qs := hr.StatStarSegment()

	graph := BuildSemanticGraph(qs)
	//assert.Equal(t, graph.nodeNum, uint64(231))
	//assert.Equal(t, graph.edgeNum, uint64(2377))

	cacheInstance := make([]*CacheInstance, 0)
	for i := 0; i < 5; i++ {
		cacheInstance = append(cacheInstance, NewCacheInstance(nil))
	}
	avgLoad := qs.AvgLoad()
	CopyNum(qs, graph, avgLoad)
	assert.Equal(t, avgLoad, float64(115))
	assert.Equal(t, avgLoad*float64(len(qs.StatInfo)), float64(26565)) // 231 * 230 / 2

	//for _, node := range graph.hashTable {
	//	SelectInstance(avgLoad, node, cacheInstance, qs)
	//}

	nodes := TraverseSort(graph)
	for _, node := range nodes {
		SelectInstance(avgLoad, node, cacheInstance, qs)
	}

	for i, c := range cacheInstance {
		fmt.Printf("load : %f , node num : %d\n", cacheInstance[i].Load, len(cacheInstance[i].Nodes))
		tmpLoad := float64(0)
		for _, node := range c.Nodes {
			tmpLoad += float64(qs.StatInfo[node.hashCode]) / float64(node.copyNum)
		}
		assert.Equal(t, tmpLoad, c.Load)
	}

}
