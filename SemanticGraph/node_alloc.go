package SemanticGraph

import (
	"math"
	"slices"
	"sort"

	stscache "github.com/timescale/tsbs/kwdb_cache_client/stscache_client"
)

func GetAggrSecond(intervalType Aggregation_Interval_Type, intervalLength uint32) uint32 {
	switch uint8(intervalType) {
	case SECOND:
		return intervalLength
	case MINUTE:
		return intervalLength * 60
	case HOUR:
		return intervalLength * 60 * 60
	case DAY:
		return intervalLength * 60 * 60 * 24
	case WEEK:
		return intervalLength * 60 * 60 * 24 * 7
	case MONTH:
		return intervalLength * 60 * 60 * 24 * 7 * 30
	case YEAR:
		return intervalLength * 60 * 60 * 24 * 7 * 30 * 12
	default:
		return 10
	}
}

func CopyNum(qs *QueryStat, g *SemanticGraph, avgLoad float64) {
	for _, node := range g.hashTable {
		node.copyNum = uint32(math.Ceil(float64(qs.StatInfo[node.hashCode]) / avgLoad))
		if node.copyNum == 0 {
			node.copyNum = 1
		}
	}
}

type qstat struct {
	starSegment string
	nodeLoad    uint64
}

func SplitHighLoadNode(qs *QueryStat, graph *SemanticGraph) {
	qsSlice := make([]*qstat, 0)
	for seg, load := range qs.StatInfo {
		qsSlice = append(qsSlice, &qstat{starSegment: seg, nodeLoad: load})
	}
	sort.Slice(qsSlice, func(i, j int) bool {
		return qsSlice[i].nodeLoad > qsSlice[j].nodeLoad
	})
	for i := 0; i < len(qsSlice)/3; i++ {
		node := graph.FindSemanticNode(qsSlice[i].starSegment)
		node.IsHighLoad = true
	}
}

func TraverseSort(g *SemanticGraph) []*SemanticNode {
	name := make([]*SemanticNode, 0)
	level := uint32(0)
	//fmt.Println(g.nodeNum)
	for len(name) < int(g.nodeNum) {
		//flag := false
		for _, node := range g.hashTable {
			if node.level == math.MaxUint32 {
				continue
			}
			if node.IsHighLoad {
				name = append(name, node)
				//flag = true
				node.level = math.MaxUint32
				continue
			}

			if node.level == level {
				node.level = math.MaxUint32
				name = append(name, node)

				for _, edge := range node.inEdges {
					if edge.sourceNode.level != math.MaxUint32 {
						edge.sourceNode.level++
					}
				}
				for _, edge := range node.outEdges {
					if edge.destinationNode.level != math.MaxUint32 {
						edge.destinationNode.level++
					}
				}
			}
			//	node.level++
		}
		//if !flag {
		//	level++
		//}
		level++
	}
	return name
}

type CacheInstance struct {
	Conn *stscache.Client
	Load float64
	//AmplifyNum float64
	Nodes []*SemanticNode
}

type CacheLoad struct {
	idx        uint32
	Load       float64
	AmplifyNum float64
}

func NewCacheInstance(conn *stscache.Client) *CacheInstance {
	return &CacheInstance{
		Conn: conn,
		Load: 0,
		//	AmplifyNum: 0,
		Nodes: make([]*SemanticNode, 0),
	}
}

func AddNodeToCacheInstance(node *SemanticNode, instance *CacheInstance, qs *QueryStat) {
	instance.Nodes = append(instance.Nodes, node)
	instance.Load += float64(qs.StatInfo[node.hashCode]) / float64(node.copyNum)
}

func SelectInstance(avgLoad float64, node *SemanticNode, instance []*CacheInstance, qs *QueryStat) {
	cacheLoad := make([]*CacheLoad, len(instance))
	for i, c := range instance {
		cacheLoad[i] = &CacheLoad{
			idx:        uint32(i),
			Load:       c.Load,
			AmplifyNum: 0,
		}
	}

	if len(instance) == 0 {
		panic("must have at least one cache instance")
		return
	}
	if len(instance) == 1 {
		for i := 0; i < int(node.copyNum); i++ {
			AddNodeToCacheInstance(node, instance[0], qs)
			node.InstanceID = append(node.InstanceID, 0)
		}
		return
	}
	sort.Slice(cacheLoad, func(i, j int) bool {
		return cacheLoad[i].Load < cacheLoad[j].Load
	})

	minLoad := cacheLoad[0].Load
	loadRange := float64(minLoad) + float64(avgLoad)*0.5/10

	candidateSet := make([]*CacheLoad, 0)
	for _, c := range cacheLoad {
		if c.Load < loadRange {
			candidateSet = append(candidateSet, c)
		} else {
			break
		}
	}
	//println("cpnum:", node.copyNum)
	if int(node.copyNum) >= len(candidateSet) || node.IsHighLoad {
		for i := 0; i < int(node.copyNum); i++ {
			AddNodeToCacheInstance(node, instance[cacheLoad[i].idx], qs)
			node.InstanceID = append(node.InstanceID, cacheLoad[i].idx)
		}
		return
	}

	for _, c := range candidateSet {
		c.AmplifyNum = 0
		amplNum := float64(0)
		for _, ie := range node.inEdges {
			if slices.Contains(instance[c.idx].Nodes, ie.sourceNode) {
				amplNum += (float64(len(ie.sourceNode.metadata.columns)) / float64(len(node.metadata.columns))) /
					(float64(GetAggrSecond(ie.sourceNode.aggregationIntervalType, ie.sourceNode.aggregationIntervalLength)) /
						float64(GetAggrSecond(node.aggregationIntervalType, node.aggregationIntervalLength)))
				if ie.sourceNode.predicates == nil && node.predicates != nil {
					amplNum *= 100
				}
			}
			//	fmt.Print(amplNum)
		}
		for _, oe := range node.outEdges {
			if slices.Contains(instance[c.idx].Nodes, oe.destinationNode) {
				amplNum += float64(len(node.metadata.columns)) / float64(len(oe.destinationNode.metadata.columns)) /
					float64(GetAggrSecond(node.aggregationIntervalType, node.aggregationIntervalLength)) /
					float64(GetAggrSecond(oe.destinationNode.aggregationIntervalType, oe.destinationNode.aggregationIntervalLength))
				if node.predicates == nil && oe.destinationNode.predicates != nil {
					amplNum *= 100
				}
			}
			//	fmt.Print(amplNum)
		}
		if amplNum == 0 {
			c.AmplifyNum = 0
		} else {
			c.AmplifyNum += float64(1) / amplNum
		}
	}

	sort.Slice(candidateSet, func(i, j int) bool {
		return candidateSet[i].AmplifyNum < candidateSet[j].AmplifyNum
	})
	//	println("cpnum:", node.copyNum)
	for i := 0; i < int(node.copyNum); i++ {
		AddNodeToCacheInstance(node, instance[candidateSet[i].idx], qs)
		node.InstanceID = append(node.InstanceID, candidateSet[i].idx)
	}
	return
}
