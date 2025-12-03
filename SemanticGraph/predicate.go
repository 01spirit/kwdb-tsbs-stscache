package SemanticGraph

import "strconv"

type PREDICATE_OP_TYPE uint8

const (
	Greater = iota
	Greater_And_Equal
	Equal
	Less
	Less_And_Equal
)

type OpNumValue struct {
	value interface{}
}

type Predicate struct {
	column     *Column
	opType     PREDICATE_OP_TYPE
	isString   bool
	opNumValue OpNumValue
	opStrValue string
}

type Predicates struct {
	vPres   []*Predicate
	presSet map[*Predicate]struct{}
	index   map[string]map[PREDICATE_OP_TYPE]uint32
}

func CompareOpNumValue(num1, num2 OpNumValue, datatype uint8) int {
	if datatype == INTEGER {
		if num1.value.(int) < num2.value.(int) {
			return -1
		} else if num1.value.(int) == num2.value.(int) {
			return 0
		} else {
			return 1
		}
	} else if datatype == DECIMAL {
		if num1.value.(float64) < num2.value.(float64) {
			return -1
		} else if num1.value.(float64) == num2.value.(float64) {
			return 0
		} else {
			return 1
		}
	} else {
		b1, ok1 := num1.value.(bool)
		b2, ok2 := num2.value.(bool)
		if ok1 && ok2 && b1 == b2 {
			return 0
		} else if ok1 {
			return 1
		} else {
			return -1
		}
	}
}

func NewPredicate(column *Column, opType PREDICATE_OP_TYPE, opNumValue OpNumValue, opStrValue string) *Predicate {
	if column == nil {
		return &Predicate{
			column:     nil,
			opType:     opType,
			isString:   false,
			opNumValue: OpNumValue{},
			opStrValue: "",
		}
	}
	if column.GetType() == DATA_TYPE(VARCHAR) {
		return &Predicate{
			column:     column,
			opType:     opType,
			isString:   true,
			opNumValue: opNumValue,
			opStrValue: opStrValue,
		}
	}
	return &Predicate{
		column:     column,
		opType:     opType,
		isString:   false,
		opNumValue: opNumValue,
		opStrValue: "",
	}
}

func BuildPredicate(pre *Predicate) *Predicate {
	return &Predicate{
		column:     pre.column,
		opType:     pre.opType,
		isString:   pre.isString,
		opNumValue: pre.opNumValue,
		opStrValue: pre.opStrValue,
	}
}

func (p *Predicate) Assignment(pre *Predicate) *Predicate {
	if p == pre {
		return p
	}
	p.column = pre.column
	p.opType = pre.opType
	p.isString = pre.isString
	p.opStrValue = pre.opStrValue
	p.opNumValue = pre.opNumValue
	return p
}

func (p *Predicate) IsEqual(pre *Predicate) bool {
	if !(p.column == pre.column) {
		return false
	}
	if p.opType != pre.opType {
		return false
	}
	if p.isString != pre.isString {
		return false
	}
	if p.isString {
		if p.opStrValue != pre.opStrValue {
			return false
		}
	} else {
		if CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType())) != 0 {
			return false
		}
	}
	return true
}

func (p *Predicate) Lessthan(pre *Predicate) bool {
	if p.column.GetName() == pre.column.GetName() {
		if p.opType == pre.opType {
			if p.isString {
				return p.opStrValue < pre.opStrValue
			} else {
				return CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType())) == -1
			}
		} else {
			return p.opType < pre.opType
		}
	}
	return p.column.GetName() < pre.column.GetName()
}

func (p *Predicate) IsContained(pre *Predicate) bool {
	if pre == nil {
		return true
	}
	if p.column.GetName() == pre.column.GetName() {
		if p.opType == Greater && (pre.opType == Greater || pre.opType == Greater_And_Equal) {
			x := CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType()))
			if x == -1 || x == 0 {
				return true
			}
		} else if p.opType == Greater_And_Equal && (pre.opType == Greater || pre.opType == Greater_And_Equal) {
			x := CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType()))
			if pre.opType == Greater_And_Equal {
				if x == -1 || x == 0 {
					return true
				}
			} else {
				if x == -1 {
					return true
				}
			}
		} else if p.opType == Equal && pre.opType == Equal {
			if p.isString {
				if p.opStrValue == pre.opStrValue {
					return true
				}
			} else {
				x := CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType()))
				if x == 0 {
					return true
				}
			}
		} else if p.opType == Less && (pre.opType == Less || pre.opType == Less_And_Equal) {
			x := CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType()))
			if x == 0 || x == 1 {
				return true
			}
		} else if p.opType == Less_And_Equal && (pre.opType == Less || pre.opType == Less_And_Equal) {
			x := CompareOpNumValue(p.opNumValue, pre.opNumValue, uint8(p.column.GetType()))
			if pre.opType == Less {
				if x == 0 || x == 1 {
					return true
				}
			} else {
				if x == 1 {
					return true
				}
			}
		}
	}
	return false
}

func (p *Predicate) String() string {
	str := ""
	str += p.column.GetName()
	switch p.opType {
	case Greater:
		str += " > "
	case Greater_And_Equal:
		str += " >= "
	case Equal:
		str += " = "
	case Less_And_Equal:
		str += " <= "
	case Less:
		str += " < "
	}

	if p.isString {
		str += p.opStrValue
	} else if p.column.GetType() == DATA_TYPE(BOOLEAN) {
		if p.opNumValue.value.(bool) {
			str += "true"
		} else {
			str += "false"
		}
	} else if p.column.GetType() == DATA_TYPE(INTEGER) || p.column.GetType() == DATA_TYPE(BIGINT) {
		str += strconv.Itoa(p.opNumValue.value.(int))
	} else if p.column.GetType() == DATA_TYPE(DECIMAL) {
		str += strconv.FormatFloat(p.opNumValue.value.(float64), 'g', -1, 64)
	}

	return str
}

func NewPredicates(pres []*Predicate) *Predicates {
	vPres := make([]*Predicate, 0)
	presSet := make(map[*Predicate]struct{})
	index := make(map[string]map[PREDICATE_OP_TYPE]uint32)
	for i := range pres {
		vPres = append(vPres, pres[i])
		presSet[pres[i]] = struct{}{}
		key := pres[i].column.GetName()
		if _, ok := index[key]; !ok {
			tmp := make(map[PREDICATE_OP_TYPE]uint32)
			index[key] = tmp
		}
		index[key][pres[i].opType] = uint32(i)
	}
	return &Predicates{
		vPres:   vPres,
		presSet: presSet,
		index:   index,
	}
}

func BuildPredicates(pres *Predicates) *Predicates {
	return &Predicates{
		vPres:   pres.vPres,
		presSet: pres.presSet,
		index:   pres.index,
	}
}

func (p *Predicates) Assignment(pres *Predicates) *Predicates {
	if p == pres {
		return p
	}
	p.vPres = pres.vPres
	p.index = pres.index
	return p
}

func (p *Predicates) IsTotallyContained(pres *Predicates) bool {
	for i := uint32(0); i < uint32(len(pres.vPres)); i++ {
		true_flag := 0
		temp := p.index[pres.vPres[i].column.GetName()][pres.vPres[i].opType]
		if temp == 0 {
			var tempOpValue PREDICATE_OP_TYPE
			if pres.vPres[i].opType == Less {
				tempOpValue = Less_And_Equal
			} else if pres.vPres[i].opType == Less_And_Equal {
				tempOpValue = Less
			} else if pres.vPres[i].opType == Greater {
				tempOpValue = Greater_And_Equal
			} else if pres.vPres[i].opType == Greater_And_Equal {
				tempOpValue = Greater
			}
			if _, ok := p.index[pres.vPres[i].column.GetName()]; !ok {
				continue
			}
			if _, ok := p.index[pres.vPres[i].column.GetName()][tempOpValue]; !ok {
				continue
			}
			temp = p.index[pres.vPres[i].column.GetName()][tempOpValue]
		}
		if temp == 0 {
			continue
		}
		tempPtr := BuildPredicate(pres.vPres[i])
		for _, it := range p.vPres {
			if it.IsContained(tempPtr) == true {
				true_flag = 1
			}
		}
		if true_flag == 0 {
			return false
		}
	}
	if len(p.vPres) > len(pres.vPres) {
		return false
	}
	return true
}
