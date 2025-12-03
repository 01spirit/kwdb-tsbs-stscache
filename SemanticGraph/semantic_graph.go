package SemanticGraph

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const (
	SemanticGraphAlgo    = "semantic graph"
	MemcacheSelectorAlgo = "memcache"
)

var CInstanceSlice []*CacheInstance
var BuildGraphNum uint64
var SGraph *SemanticGraph
var HisRecord *HistoryRecord
var QStat *QueryStat
var FinishBuildGraph bool
var DistAlgo string = SemanticGraphAlgo

type SchemaRef Schema

type SemanticNode struct {
	metricNameSet             map[string]struct{}
	metricName                string
	hashCode                  string
	metadata                  *Schema
	fieldNameSet              map[string]struct{}
	tagVisitTime              map[string]uint32
	outEdges                  []*SemanticEdge
	inEdges                   []*SemanticEdge
	predicates                *Predicates
	aggregationType           Aggregation_Type
	aggregationIntervalType   Aggregation_Interval_Type
	aggregationIntervalLength uint32

	//
	InstanceID []uint32
	copyNum    uint32
	level      uint32
	IsHighLoad bool
}

type EdgeWeight struct {
	edgeWeightType       EDGE_WEIGHT_TYPE
	edgeComputeType      EDGE_COMPUTE_TYPE
	edgeIntersectionType EDGE_INTERSECTION_TYPE
	subMetadata          *Schema
}

type SemanticEdge struct {
	sourceNode      *SemanticNode
	destinationNode *SemanticNode
	edgeWeight      EdgeWeight
}

type SemanticGraph struct {
	hashTable map[string]*SemanticNode
	nodeNum   uint64
	edgeNum   uint64
}

//************************************ SemanticNode ****************************//

func NewSemanticNode(hashCode string, metricName string, metadata *Schema, nodeIsAtomic bool, predicates *Predicates,
	aggregrationType Aggregation_Type, aggregationIntervalType Aggregation_Interval_Type, aggregationIntervalLength uint32) *SemanticNode {
	tempColumns := metadata.columns
	fieldNameSet := make(map[string]struct{})
	for i := range tempColumns {
		fieldNameSet[tempColumns[i].GetName()] = struct{}{}
	}
	return &SemanticNode{
		metricNameSet:             nil,
		metricName:                metricName,
		hashCode:                  hashCode,
		metadata:                  metadata,
		fieldNameSet:              fieldNameSet,
		tagVisitTime:              make(map[string]uint32),
		outEdges:                  nil,
		inEdges:                   nil,
		predicates:                predicates,
		aggregationType:           aggregrationType,
		aggregationIntervalType:   aggregationIntervalType,
		aggregationIntervalLength: aggregationIntervalLength,
		InstanceID:                make([]uint32, 0),
		copyNum:                   1,
		level:                     0,
		IsHighLoad:                false,
	}
}

func BuildSemanticNode(sem *SemanticNode) *SemanticNode {
	return &SemanticNode{
		metricNameSet:             sem.metricNameSet,
		metricName:                sem.metricName,
		hashCode:                  sem.hashCode,
		metadata:                  sem.metadata,
		fieldNameSet:              sem.fieldNameSet,
		tagVisitTime:              make(map[string]uint32),
		outEdges:                  sem.outEdges,
		inEdges:                   sem.inEdges,
		predicates:                sem.predicates,
		aggregationType:           sem.aggregationType,
		aggregationIntervalType:   sem.aggregationIntervalType,
		aggregationIntervalLength: sem.aggregationIntervalLength,
		InstanceID:                sem.InstanceID,
		copyNum:                   sem.copyNum,
		level:                     sem.level,
		IsHighLoad:                sem.IsHighLoad,
	}
}

func (n *SemanticNode) AddInEdge(edge *SemanticEdge) {
	n.inEdges = append(n.inEdges, edge)
}

func (n *SemanticNode) AddOutEdge(edge *SemanticEdge) {
	n.outEdges = append(n.outEdges, edge)
}

func (n *SemanticNode) GetInEdges() []*SemanticEdge {
	return n.inEdges
}

func (n *SemanticNode) GetOutEdge() []*SemanticEdge {
	return n.outEdges
}

func (n *SemanticNode) GetHashCode() string {
	return n.hashCode
}

func (n *SemanticNode) GetCopyNum() uint32 {
	return n.copyNum
}

func (n *SemanticNode) GetRowLength() uint32 {
	return n.metadata.GetLength()
}

func (n *SemanticNode) GetMetricNameSet() map[string]struct{} {
	return n.metricNameSet
}

func (n *SemanticNode) GetMetricName() string {
	return n.metricName
}

func (n *SemanticNode) GetMetadataRef() *Schema {
	return n.metadata
}

func (n *SemanticNode) GetPredicates() *Predicates {
	return n.predicates
}

func (n *SemanticNode) GetFieldNameSet() map[string]struct{} {
	return n.fieldNameSet
}

func (n *SemanticNode) GetAggregationType() Aggregation_Type {
	return n.aggregationType
}

func (n *SemanticNode) GetAggregationIntervalType() Aggregation_Interval_Type {
	return n.aggregationIntervalType
}

func (n *SemanticNode) GetAggregationIntervalLength() uint32 {
	return n.aggregationIntervalLength
}

func (n *SemanticNode) IncTagVisitTime(tags []string) {
	for _, tag := range tags {
		//if _, ok := n.tagVisitTime[tag]; !ok {
		//	n.tagVisitTime[tag] = 1
		//} else {
		//	n.tagVisitTime[tag]++
		//}
		n.tagVisitTime[tag]++
	}
}

func (n *SemanticNode) PrintSemanticNode() {
	fmt.Print("=====================================\n")
	fmt.Printf("hash code: %s\n", n.hashCode)
	fmt.Print("=====================================\n")
	fmt.Println("Semantic Segment")
	fmt.Printf("Metric:\n%s\n", n.metricName)
	fmt.Println("Field:")
	tmpKeys := make([]string, len(n.fieldNameSet))
	for key := range n.fieldNameSet {
		tmpKeys = append(tmpKeys, key)
	}
	slices.Sort(tmpKeys)
	for _, key := range tmpKeys {
		fmt.Printf("%s ", key)
	}
	fmt.Println("\nPredicates:")
	if n.predicates != nil {
		for _, pre := range n.predicates.vPres {
			fmt.Println(pre.String())
		}
	} else {
		fmt.Println("No Predicates")
	}
	fmt.Println("Aggregation:")
	fmt.Printf("Aggregation Type: %s Aggregation Interval Type: %s Aggregation Interval Length: %d\n",
		PrintAggregationType(n.aggregationType),
		PrintAggregationIntervalType(n.aggregationIntervalType),
		n.aggregationIntervalLength)

	fmt.Println()
	fmt.Println("copy num: ", n.copyNum)
	fmt.Println("level: ", n.level)
}

//************************************ EdgeWeight ****************************//

func NewEdgeWeight(eWightType EDGE_WEIGHT_TYPE, eComputeType EDGE_COMPUTE_TYPE, eIntersectionType EDGE_INTERSECTION_TYPE,
	subMetadata *Schema) *EdgeWeight {
	return &EdgeWeight{
		edgeWeightType:       eWightType,
		edgeComputeType:      eComputeType,
		edgeIntersectionType: eIntersectionType,
		subMetadata:          subMetadata,
	}
}

func BuildEdgeWeight(e *EdgeWeight) *EdgeWeight {
	return &EdgeWeight{
		edgeWeightType:       e.edgeWeightType,
		edgeComputeType:      e.edgeComputeType,
		edgeIntersectionType: e.edgeIntersectionType,
		subMetadata:          e.subMetadata,
	}
}

func (w *EdgeWeight) Assignment(weight *EdgeWeight) *EdgeWeight {
	if w == weight {
		return w
	}
	w.edgeComputeType = weight.edgeComputeType
	w.edgeIntersectionType = weight.edgeIntersectionType
	w.edgeWeightType = weight.edgeWeightType
	w.subMetadata = weight.subMetadata
	return w
}

//************************************ SemanticEdge ****************************//

func NewSemanticEdge(sourceNode, destNode *SemanticNode, edgeWeight *EdgeWeight) *SemanticEdge {
	return &SemanticEdge{
		sourceNode:      sourceNode,
		destinationNode: destNode,
		edgeWeight:      *edgeWeight,
	}
}

func (e *SemanticEdge) GetSourceNode() *SemanticNode {
	return e.sourceNode
}

func (e *SemanticEdge) GetDestinationNode() *SemanticNode {
	return e.destinationNode
}

func (e *SemanticEdge) GetSourceNodeHashCode() string {
	return e.sourceNode.hashCode
}

func (e *SemanticEdge) GetDestinationNodeHashCode() string {
	return e.destinationNode.hashCode
}

func (e *SemanticEdge) GetEdgeWeight() *EdgeWeight {
	return &e.edgeWeight
}

//************************************ SemanticGraph ****************************//

func NewSemanticGraph() *SemanticGraph {
	return &SemanticGraph{
		hashTable: make(map[string]*SemanticNode),
		nodeNum:   0,
		edgeNum:   0,
	}
}

func SplitSegment(input string) ([]string, string, []string) {
	startPos := strings.Index(input, "{")
	endPos := strings.Index(input, "}")
	if startPos < endPos {
		sm := input[startPos+1 : endPos]
		other := input[endPos:]
		segments, tags := RebuildSegment(sm, other)
		star := "{(" + GetMetricName(sm) + ".*)" + other
		return segments, star, tags
	}
	return nil, "", nil
}

func RebuildSegment(input, other string) (result []string, tags []string) {
	splitSM := strings.Split(input, ")")
	//tags = make([]string, len(splitSM))
	for _, sm := range splitSM {
		if len(sm) > 0 {
			eqIdx := strings.Index(sm, "=")
			tags = append(tags, sm[eqIdx+1:])
			content := "{" + sm + ")" + other
			result = append(result, content)
		}
	}
	return result, tags
}

func GetMetricName(input string) string {
	leftPos := strings.Index(input, "(")
	dotPos := strings.Index(input, ".")
	if leftPos >= 0 && dotPos > 0 && leftPos < dotPos {
		return input[leftPos+1 : dotPos]
	}
	return ""
}

func ExtractBraceContents(input string) []string {
	reslut := make([]string, 0)
	startPos := strings.Index(input, "{")
	endPos := strings.Index(input, "}")
	for startPos >= 0 && endPos >= 0 {
		if startPos < endPos {
			content := input[startPos+1 : endPos]
			reslut = append(reslut, content)
		}
		input = input[endPos+1:]
		startPos = strings.Index(input, "{")
		endPos = strings.Index(input, "}")
	}
	return reslut
}

func ExtractContentInParentheses(input string) []string {
	result := make([]string, 0)
	startPos := strings.Index(input, "(")
	endPos := strings.Index(input, ")")
	for startPos >= 0 && endPos >= 0 {
		if startPos < endPos {
			content := input[startPos+1 : endPos]
			result = append(result, content)
		}
		input = input[endPos+1:]
		startPos = strings.Index(input, "(")
		endPos = strings.Index(input, ")")
	}
	return result
}

type InequalityElements struct {
	variable string
	op       string
	constant string
}

func ExtractVariablesSymbolConstant(inequality string) InequalityElements {
	pattern1 := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*([=<>]+)\s*([-+]?[0-9]*\.?[0-9]+)`)
	pattern2 := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*) ([<>=!]+) ([a-zA-Z0-9]+)`)

	match := pattern1.FindStringSubmatch(inequality)
	if match != nil {
		return InequalityElements{match[1], match[2], match[3]}
	}

	match = pattern2.FindStringSubmatch(inequality)
	if match != nil {
		return InequalityElements{match[1], match[2], match[3]}
	}

	return InequalityElements{"", "", ""}
}

func (g *SemanticGraph) AddEdge(newEdge *SemanticEdge) {
	g.hashTable[newEdge.GetSourceNodeHashCode()].AddOutEdge(newEdge)
	g.hashTable[newEdge.GetDestinationNodeHashCode()].AddInEdge(newEdge)
	g.edgeNum++
}

var nodelock sync.RWMutex

func (g *SemanticGraph) AddNode(newNode *SemanticNode) {
	//nodelock.Lock()
	g.hashTable[newNode.GetHashCode()] = newNode
	//nodelock.Unlock()
	g.BuildRelationships(newNode)
	g.nodeNum++
	//nodelock.Unlock()
}

func (g *SemanticGraph) GetNodeNum() uint32 {
	return uint32(g.nodeNum)
}

func (g *SemanticGraph) GetEdgeNum() uint32 {
	return uint32(g.edgeNum)
}

func (g *SemanticGraph) FindSemanticNode(hashCode string) *SemanticNode {
	return g.hashTable[hashCode]
}

func (g *SemanticGraph) IsExistNode(hashCode string) bool {
	//nodelock.Lock()
	_, exist := g.hashTable[hashCode]
	//nodelock.Unlock()
	return exist
}

func (g *SemanticGraph) GetHahsTable() map[string]*SemanticNode {
	return g.hashTable
}

func ColumnNum(segment string) int {
	segments := ExtractBraceContents(segment)
	fieldStr := strings.Split(segments[1], ",")

	//cols := make([]*Column, 0)
	//for _, temp := range fieldStr {
	//	start := strings.Index(temp, "[")
	//	end := strings.Index(temp, "]")
	//	fieldName := temp[:start]
	//	fieldType := temp[start+1 : end]
	//
	//	var dataType DATA_TYPE
	//	switch fieldType {
	//	case "float64":
	//		dataType = DATA_TYPE(DECIMAL)
	//	case "int64":
	//		dataType = DATA_TYPE(BIGINT)
	//	case "bool":
	//		dataType = DATA_TYPE(BOOLEAN)
	//	default:
	//		dataType = DATA_TYPE(VARCHAR)
	//	}
	//	cols = append(cols, NewColumn(fieldName, dataType))
	//}
	return len(fieldStr)
}

func SemanticSegmentToNode(segment string) *SemanticNode {
	segments := ExtractBraceContents(segment)
	nodeIsAtomic := false
	if strings.Count(segments[0], "(") == 1 {
		nodeIsAtomic = true
	}

	// SF ---> metadata
	cols := make([]*Column, 0)
	fieldStr := strings.Split(segments[1], ",")
	for _, temp := range fieldStr {
		start := strings.Index(temp, "[")
		end := strings.Index(temp, "]")
		fieldName := temp[:start]
		fieldType := temp[start+1 : end]

		var dataType DATA_TYPE
		switch fieldType {
		case "float64":
			dataType = DATA_TYPE(DECIMAL)
		case "int64":
			dataType = DATA_TYPE(BIGINT)
		case "bool":
			dataType = DATA_TYPE(BOOLEAN)
		default:
			dataType = DATA_TYPE(VARCHAR)
		}
		cols = append(cols, NewColumn(fieldName, dataType))
	}
	metadata := NewSchema(cols)

	// SP
	var preds []*Predicate
	var predicates *Predicates
	if segments[2] != "empty" {
		predStr := ExtractContentInParentheses(segments[2])
		for _, temp := range predStr {
			start := strings.Index(temp, "[")
			end := strings.Index(temp, "]")
			fieldName := temp[:start]
			fieldType := temp[start+1 : end]
			element := ExtractVariablesSymbolConstant(fieldName)
			var opType PREDICATE_OP_TYPE
			var datatype DATA_TYPE
			var constant OpNumValue
			switch element.op {
			case ">":
				opType = Greater
			case ">=":
				opType = Greater_And_Equal
			case "=":
				opType = Equal
			case "<=":
				opType = Less_And_Equal
			default:
				opType = Less
			}

			if fieldType == "string" {
				col := NewColumn(element.variable, DATA_TYPE(VARCHAR))
				pred := NewPredicate(col, opType, constant, element.constant)
				preds = append(preds, pred)
			} else {
				if fieldType == "float64" {
					datatype = DATA_TYPE(DECIMAL)
					constant.value, _ = strconv.ParseFloat(element.constant, 64)
				} else if fieldType == "int64" {
					datatype = DATA_TYPE(BIGINT)
					constant.value, _ = strconv.Atoi(element.constant)
				} else {
					datatype = DATA_TYPE(BOOLEAN)
					if element.constant == "false" {
						constant.value = false
					} else {
						constant.value = true
					}
				}
				col := NewColumn(element.variable, datatype)
				pred := NewPredicate(col, opType, constant, element.constant)
				preds = append(preds, pred)
			}
		}
		predicates = NewPredicates(preds)
	}

	// SG
	aggType := Aggregation_Type(NONE)
	aggIntervalType := Aggregation_Interval_Type(NATURAL)
	aggIntervalLength := uint64(1)
	aggregateStr := strings.Split(segments[3], ",")
	if aggregateStr[0] != "empty" {
		switch aggregateStr[0] {
		case "max":
			aggType = Aggregation_Type(MAX)
		case "min":
			aggType = Aggregation_Type(MIN)
		case "count":
			aggType = Aggregation_Type(COUNT)
		case "sum":
			aggType = Aggregation_Type(SUM)
		case "mean":
			aggType = Aggregation_Type(MEAN)
		default:
			aggType = Aggregation_Type(NONE)
		}
		if aggregateStr[1] != "empty" {
			aggIntervalLength, _ = strconv.ParseUint(aggregateStr[1][:len(aggregateStr[1])-1], 10, 32)
			switch aggregateStr[1][len(aggregateStr[1])-1:] {
			case "s":
				aggIntervalType = Aggregation_Interval_Type(SECOND)
			case "m":
				aggIntervalType = Aggregation_Interval_Type(MINUTE)
			case "h":
				aggIntervalType = Aggregation_Interval_Type(HOUR)
			case "D":
				aggIntervalType = Aggregation_Interval_Type(DAY)
			case "M":
				aggIntervalType = Aggregation_Interval_Type(MONTH)
			case "Y":
				aggIntervalType = Aggregation_Interval_Type(YEAR)
			}
		}
	}
	if predicates != nil {
		return NewSemanticNode(segment, segments[0], metadata, nodeIsAtomic, predicates, aggType, aggIntervalType, uint32(aggIntervalLength))
	} else {
		return NewSemanticNode(segment, segments[0], metadata, nodeIsAtomic, nil, aggType, aggIntervalType, uint32(aggIntervalLength))
	}
}

func isSubset(set1, set2 map[string]struct{}) bool {
	for key := range set1 {
		if _, exist := set2[key]; !exist {
			return false
		}
	}
	return true
}

func IntersectionSet(set1, set2 map[string]struct{}) map[string]struct{} {
	intersectionSet := make(map[string]struct{})
	for key := range set1 {
		if _, exist := set2[key]; exist {
			intersectionSet[key] = struct{}{}
		}
	}
	return intersectionSet
}

func (g *SemanticGraph) FindAtomicRelationships(newNode *SemanticNode, node *SemanticNode) (*EdgeWeight, *EdgeWeight) {
	if newNode == nil || node == nil {
		return nil, nil
	}
	if newNode.GetMetricName() != node.GetMetricName() {
		return nil, nil
	}
	if newNode.aggregationType != node.aggregationType && newNode.aggregationType != Aggregation_Type(NONE) && node.aggregationType != Aggregation_Type(NONE) {
		return nil, nil
	}

	subFieldSet := IntersectionSet(newNode.fieldNameSet, node.fieldNameSet)
	if len(subFieldSet) == 0 {
		return nil, nil
	}

	intersectionType1 := Intersection
	intersectionType2 := Intersection
	flag1 := isSubset(newNode.fieldNameSet, node.fieldNameSet)
	flag2 := isSubset(node.fieldNameSet, newNode.fieldNameSet)
	if flag1 == true && flag2 == false {
		intersectionType1 = My_Subset
		intersectionType2 = Totally_Contained
	}
	if flag1 == false && flag2 == true {
		intersectionType1 = Totally_Contained
		intersectionType2 = My_Subset
	}
	if flag1 == true && flag2 == true {
		intersectionType1 = Same
		intersectionType2 = Same
	}
	subSchema := SubSchema(newNode.metadata, node.metadata)
	if newNode.aggregationType == node.aggregationType {
		if (newNode.predicates == nil && node.predicates == nil) || newNode.predicates == node.predicates {
			if newNode.aggregationIntervalType < node.aggregationIntervalType || (newNode.aggregationIntervalType == node.aggregationIntervalType && newNode.aggregationIntervalLength < node.aggregationIntervalLength) {
				if intersectionType1 == Totally_Contained || intersectionType1 == Same {
					edgeWeightPtr := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Aggregation_Compute), EDGE_INTERSECTION_TYPE(intersectionType1), subSchema)
					return edgeWeightPtr, nil
				} else {
					return nil, nil
				}

			} else if newNode.aggregationIntervalType > node.aggregationIntervalType || (newNode.aggregationIntervalType == node.aggregationIntervalType && newNode.aggregationIntervalLength > node.aggregationIntervalLength) {
				if intersectionType2 == Totally_Contained || intersectionType2 == Same {
					edgeWeightPtr := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Aggregation_Compute), EDGE_INTERSECTION_TYPE(intersectionType2), subSchema)
					return nil, edgeWeightPtr
				} else {
					return nil, nil
				}

			} else {
				var edgeWeightPtr1 *EdgeWeight = nil
				if intersectionType1 == Totally_Contained || intersectionType1 == Same {
					edgeWeightPtr1 = NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Not_Compute), EDGE_INTERSECTION_TYPE(intersectionType1), subSchema)
				}
				var edgeWeightPtr2 *EdgeWeight = nil
				if intersectionType2 == Totally_Contained || intersectionType2 == Same {
					edgeWeightPtr2 = NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Not_Compute), EDGE_INTERSECTION_TYPE(intersectionType2), subSchema)
				}
				return edgeWeightPtr1, edgeWeightPtr2
			}
		} else {
			if newNode.aggregationIntervalType == node.aggregationIntervalType {
				if newNode.predicates == nil && node.predicates != nil {
					if intersectionType1 == Totally_Contained || intersectionType1 == Same {
						edgeWeightPtr := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Predicate_Compute), EDGE_INTERSECTION_TYPE(intersectionType1), subSchema)
						return edgeWeightPtr, nil
					} else {
						return nil, nil
					}
				} else if newNode.predicates != nil && node.predicates == nil {
					if intersectionType2 == Totally_Contained || intersectionType2 == Same {
						edgeWeightPtr := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Predicate_Compute), EDGE_INTERSECTION_TYPE(intersectionType2), subSchema)
						return nil, edgeWeightPtr
					} else {
						return nil, nil
					}
				} else {
					if newNode.predicates.IsTotallyContained(node.predicates) {
						if intersectionType2 == Totally_Contained || intersectionType2 == Same {
							edgeWeightPtr := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Predicate_Compute), EDGE_INTERSECTION_TYPE(intersectionType2), subSchema)
							return nil, edgeWeightPtr
						} else {
							return nil, nil
						}
					} else if node.predicates.IsTotallyContained(newNode.predicates) {
						if intersectionType1 == Totally_Contained || intersectionType1 == Same {
							edgeWeightPtr := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Predicate_Compute), EDGE_INTERSECTION_TYPE(intersectionType1), subSchema)
							return edgeWeightPtr, nil
						} else {
							return nil, nil
						}
					}
				}
			}
		}
	} else if newNode.aggregationType == Aggregation_Type(NONE) || node.aggregationType == Aggregation_Type(NONE) {
		if (newNode.predicates == nil && node.predicates == nil) || newNode.predicates == node.predicates {
			if newNode.aggregationType == Aggregation_Type(NONE) {
				if intersectionType1 == Totally_Contained || intersectionType1 == Same {
					edgeWeightPtr1 := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Aggregation_Compute), EDGE_INTERSECTION_TYPE(intersectionType1), subSchema)
					return edgeWeightPtr1, nil
				} else {
					return nil, nil
				}
			} else {
				if intersectionType2 == Totally_Contained || intersectionType2 == Same {
					edgeWeightPtr2 := NewEdgeWeight(EDGE_WEIGHT_TYPE(Atomic_To_Atomic), EDGE_COMPUTE_TYPE(Aggregation_Compute), EDGE_INTERSECTION_TYPE(intersectionType2), subSchema)
					return nil, edgeWeightPtr2
				} else {
					return nil, nil
				}
			}
		}
	}

	return nil, nil
}

func (g *SemanticGraph) BuildRelationships(newNode *SemanticNode) {
	if newNode == nil {
		return
	}
	for key, node := range g.hashTable {
		if newNode.hashCode == key {
			continue
		}
		ew1, ew2 := g.FindAtomicRelationships(newNode, node)
		if ew1 != nil {
			edgePtr := NewSemanticEdge(newNode, node, ew1)
			g.AddEdge(edgePtr)
		}
		if ew2 != nil {
			edgePtr := NewSemanticEdge(node, newNode, ew2)
			g.AddEdge(edgePtr)
		}
	}
}

func (g *SemanticGraph) AddNodeFromSegment(segment string) {
	_, star, tags := SplitSegment(segment)
	nodelock.Lock()
	if !g.IsExistNode(star) {
		node := SemanticSegmentToNode(star)
		g.AddNode(node)
	}
	node := g.FindSemanticNode(star)
	node.IncTagVisitTime(tags)

	nodelock.Unlock()
}

func (g *SemanticGraph) BuildSemanticGraph(starSegment string) {
	node := SemanticSegmentToNode(starSegment)
	g.AddNode(node)
	return
}

// hash map 无序，会随机顺序输出节点
func (g *SemanticGraph) PrintGraph() {
	for _, node := range g.hashTable {
		node.PrintSemanticNode()
		fmt.Println()
	}
}

func (g *SemanticGraph) PrintSemanticSegment() {
	segments := make([]string, 0)
	for key := range g.hashTable {
		segments = append(segments, key)
	}
	slices.Sort(segments)
	for _, s := range segments {
		fmt.Println(s)
	}
	for _, s := range segments {
		fmt.Println(g.FindSemanticNode(s).tagVisitTime)
	}
}
