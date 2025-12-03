package SemanticGraph

type DATA_TYPE uint8

const (
	INVALID_DATA_TYPE = uint8(iota)
	BOOLEAN
	TINYINT
	SMALLINT
	INTEGER
	BIGINT
	DECIMAL
	VARCHAR
	TIMESTAMP
)

type EDGE_WEIGHT_TYPE uint8

const (
	Atomic_To_Atomic = uint8(iota)
	Atomic_To_Non_Atomic
)

type EDGE_COMPUTE_TYPE uint8

const (
	Predicate_Compute = uint8(iota)
	Aggregation_Compute
	Not_Compute
)

type EDGE_INTERSECTION_TYPE uint8

const (
	Same = uint8(iota)
	My_Subset
	Intersection
	Totally_Contained
)

type Aggregation_Type uint8

const (
	MAX = uint8(iota)
	MIN
	COUNT
	SUM
	MEAN
	LAST
	FIRST
	NONE
)

type Aggregation_Interval_Type uint8

const (
	NATURAL = uint8(iota)
	SECOND
	MINUTE
	HOUR
	DAY
	WEEK
	MONTH
	YEAR
)

func TypeIdToString(type_id DATA_TYPE) string {
	switch uint8(type_id) {
	case INVALID_DATA_TYPE:
		return "INVALID"
	case BOOLEAN:
		return "BOOLEAN"
	case TINYINT:
		return "TINYINT"
	case SMALLINT:
		return "SMALLINT"
	case INTEGER:
		return "INTEGER"
	case BIGINT:
		return "BIGINT"
	case DECIMAL:
		return "DECIMAL"
	case TIMESTAMP:
		return "TIMESTAMP"
	case VARCHAR:
		return "VARCHAR"
	default:
		return "INVALID"
	}
}

func PrintAggregationType(aggrType Aggregation_Type) string {
	switch uint8(aggrType) {
	case MAX:
		return "MAX"
	case MIN:
		return "MIN"
	case COUNT:
		return "COUNT"
	case SUM:
		return "SUM"
	case MEAN:
		return "MEAN"
	case LAST:
		return "LAST"
	case FIRST:
		return "FIRST"
	default:
		return "NONE"
	}
}

func PrintAggregationIntervalType(intervalType Aggregation_Interval_Type) string {
	switch uint8(intervalType) {
	case SECOND:
		return "SECOND"
	case MINUTE:
		return "MINUTE"
	case HOUR:
		return "HOUR"
	case DAY:
		return "DAY"
	case WEEK:
		return "WEEK"
	case MONTH:
		return "MONTH"
	case YEAR:
		return "YEAR"
	default:
		return "NONE"
	}
}
