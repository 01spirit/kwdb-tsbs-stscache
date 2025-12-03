package SemanticGraph

import (
	"fmt"
	"strings"
)

var cpu_fields = []string{"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait", "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice"}

var iot_diagnostics_fields = []string{"current_load", "fuel_capacity", "fuel_state", "load_capacity", "nominal_fuel_consumption"}
var iot_readings_fields = []string{"velocity", "fuel_capacity", "fuel_consumption", "grade", "heading", "latitude", "load_capacity", "longitude", "elevation", "nominal_fuel_consumption"}
var iot_table_name = []string{"diagnostics", "readings"}

// , "min", "max"
var aggr = []string{"mean"}
var interval = []string{"15m", "60m"}

var tscl_iot_diagnostics_fields = []string{"current_load", "fuel_state", "status"}
var tscl_iot_readings_fields = []string{"velocity", "fuel_consumption", "grade", "heading", "latitude", "longitude", "elevation"}
var tsclaggr = []string{"avg"}
var tsclInterval = []int{15, 60}

var iot_predicates = map[string]string{
	"velocity":         " > 95",
	"heading":          " > 350",
	"grade":            " > 95",
	"fuel_consumption": " > 45",
}

const cpu_datatype = "int64"
const iot_datatype = "float64"

func SplitFields(fields []string) [][]string {
	queryFields := make([][]string, 0)
	//for i := 0; i < len(fields); i += 3 {
	//	queryFields = append(queryFields, []string{fields[i]})
	//}
	for i := 0; i+2 <= len(fields); i += 2 {
		queryFields = append(queryFields, []string{fields[i], fields[i+1]})
	}
	for i := 0; i+3 <= len(fields); i += 2 {
		queryFields = append(queryFields, []string{fields[i], fields[i+1], fields[i+2]})
	}
	for i := 0; i+5 <= len(fields); i += 3 {
		queryFields = append(queryFields, []string{fields[i], fields[i+1], fields[i+2], fields[i+3], fields[i+4]})
	}
	return queryFields
}

func JoinInfluxPredicate(qf []string) (string, string) {
	cnt := 0
	qryPred := ""
	segPred := ""

	for _, f := range qf {
		if p, ok := iot_predicates[f]; ok {
			qryPred += fmt.Sprintf("%s%s AND ", f, p)
			segPred += fmt.Sprintf("(%s%s[float64])", f, strings.Replace(p, " ", "", -1))
			cnt++
			if cnt == 2 {
				break
			}
		}
	}

	return qryPred, segPred
}

func JoinTimescalePredicate(qf []string) (string, string) {
	cnt := 0
	qryPred := ""
	segPred := ""

	for _, f := range qf {
		if p, ok := iot_predicates[f]; ok {
			qryPred += fmt.Sprintf("AND %s%s ", f, p)
			segPred += fmt.Sprintf("(%s%s[float64])", f, strings.Replace(p, " ", "", -1))
			cnt++
			if cnt == 2 {
				break
			}
		}
	}

	return qryPred, segPred
}

func JoinSegmentFields(field []string, datatype string) string {
	res := ""
	for _, f := range field {
		res += fmt.Sprintf("%s[%s],", f, datatype)
	}
	return res[:len(res)-1]
}

func GeneratePartSegment(queryFields [][]string, datatype string) []string {
	particalSegments := make([]string, 0)
	for _, qf := range queryFields {
		particalSegments = append(particalSegments, fmt.Sprintf("{%s}#{empty}#{empty,empty}", JoinSegmentFields(qf, datatype)))
		for _, ag := range aggr {
			for _, in := range interval {
				particalSegments = append(particalSegments, fmt.Sprintf("{%s}#{empty}#{%s,%s}", JoinSegmentFields(qf, datatype), ag, in))
			}
		}
	}
	return particalSegments
}

func GenerateCpuSegment() []string {
	queryFields := SplitFields(cpu_fields)
	segments := GeneratePartSegment(queryFields, cpu_datatype)
	return segments
}

func GenerateIotReadingsSegment() []string {
	queryFields := SplitFields(iot_readings_fields)
	segments := GeneratePartSegment(queryFields, iot_datatype)
	return segments
}

func GenerateIotDiagSegment() []string {
	queryFields := SplitFields(iot_diagnostics_fields)
	segments := GeneratePartSegment(queryFields, iot_datatype)
	return segments
}

func GenerateInfluxCpuQueryTemplate() ([]string, []string) {
	queryFields := SplitFields(cpu_fields)
	//"SELECT mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM \"cpu\" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY \"hostname\",time(%s)"
	//"SELECT usage_irq,usage_softirq,usage_steal FROM \"cpu\" WHERE %s AND usage_user > 90 AND usage_guest > 90 AND TIME >= '%s' AND TIME < '%s' GROUP BY \"hostname\""

	queryTemplate := "SELECT # FROM \"cpu\" WHERE %s AND$ TIME >= '%s' AND TIME < '%s' GROUP BY \"hostname\""

	particalSegments := make([]string, 0)
	qryTemplates := make([]string, 0)
	for _, qf := range queryFields {
		particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{(%s>95[int64])(%s>95[int64])}#{empty,empty}", JoinSegmentFields(qf, cpu_datatype), qf[0], qf[1]))
		tmp := queryTemplate
		tmp = strings.Replace(tmp, "#", strings.Join(qf, ","), 1)
		tmp = strings.Replace(tmp, "$", fmt.Sprintf(" %s > 95 AND %s > 95 AND", qf[0], qf[1]), 1)
		qryTemplates = append(qryTemplates, tmp)
		for _, ag := range aggr {
			for _, in := range interval {
				particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{empty}#{%s,%s}", JoinSegmentFields(qf, cpu_datatype), ag, in))
				flds := make([]string, 0)
				for _, f := range qf {
					flds = append(flds, fmt.Sprintf("%s(%s)", ag, f))
				}
				qryTemplates = append(qryTemplates, fmt.Sprintf("SELECT %s FROM \"cpu\" WHERE %%s AND TIME >= '%%s' AND TIME < '%%s' GROUP BY \"hostname\",time(%s)", strings.Join(flds, ","), in))
			}
		}
	}

	return qryTemplates, particalSegments
}

func GenerateInfluxIotQueryTemplate() ([]string, []string) {
	queryFields1 := SplitFields(iot_readings_fields)
	//queryFields2 := SplitFields(iot_diagnostics_fields)
	//"SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "name",time(%s)"
	//"SELECT velocity,fuel_consumption,grade FROM "readings" WHERE %s AND velocity > 90 AND fuel_consumption > 40 AND TIME >= '%s' AND TIME < '%s' GROUP BY "name""

	queryTemplate1 := "SELECT # FROM \"readings\" WHERE %s AND $ TIME >= '%s' AND TIME < '%s' GROUP BY \"name\""
	//queryTemplate2 := "SELECT # FROM \"diagnostics\" WHERE %s AND$ TIME >= '%s' AND TIME < '%s' GROUP BY \"name\""

	particalSegments := make([]string, 0)
	qryTemplates := make([]string, 0)
	for _, qf := range queryFields1 {
		qryP, segP := JoinInfluxPredicate(qf)
		if qryP != "" && segP != "" {
			particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{%s}#{empty,empty}", JoinSegmentFields(qf, iot_datatype), segP))
			tmp := queryTemplate1
			tmp = strings.Replace(tmp, "#", strings.Join(qf, ","), 1)
			tmp = strings.Replace(tmp, "$", qryP, 1)
			qryTemplates = append(qryTemplates, tmp)
		}
		for _, ag := range aggr {
			for _, in := range interval {
				particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{empty}#{%s,%s}", JoinSegmentFields(qf, iot_datatype), ag, in))
				flds := make([]string, 0)
				for _, f := range qf {
					flds = append(flds, fmt.Sprintf("%s(%s)", ag, f))
				}
				qryTemplates = append(qryTemplates, fmt.Sprintf("SELECT %s FROM \"readings\" WHERE %%s AND TIME >= '%%s' AND TIME < '%%s' GROUP BY \"name\",time(%s)", strings.Join(flds, ","), in))
			}
		}
	}

	//for _, qf := range queryFields2 {
	//	particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{(%s>95[float64])(%s>95[float64])}#{empty,empty}", JoinSegmentFields(qf, iot_datatype), qf[0], qf[1]))
	//	tmp := queryTemplate2
	//	tmp = strings.Replace(tmp, "#", strings.Join(qf, ","), 1)
	//	tmp = strings.Replace(tmp, "$", fmt.Sprintf(" %s > 95 AND %s > 95 AND", qf[0], qf[1]), 1)
	//	qryTemplates = append(qryTemplates, tmp)
	//	for _, ag := range aggr {
	//		for _, in := range interval {
	//			particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{empty}#{%s,%s}", JoinSegmentFields(qf, iot_datatype), ag, in))
	//			flds := make([]string, 0)
	//			for _, f := range qf {
	//				flds = append(flds, fmt.Sprintf("%s(%s)", ag, f))
	//			}
	//			qryTemplates = append(qryTemplates, fmt.Sprintf("SELECT %s FROM \"diagnostics\" WHERE %%s AND TIME >= '%%s' AND TIME < '%%s' GROUP BY \"name\",time(%s)", strings.Join(flds, ","), in))
	//		}
	//	}
	//}

	return qryTemplates, particalSegments
}

func GenerateTimescaleCpuQueryTemplate() ([]string, []string) {
	queryFields := SplitFields(cpu_fields)
	//"SELECT time_bucket('5 minute', time) as bucket,hostname,avg(usage_system) FROM cpu WHERE %s AND time >= '%s' AND time < '%s' GROUP BY hostname,bucket ORDER BY hostname,bucket"
	//"SELECT time as bucket,hostname,usage_user,usage_system,usage_idle FROM cpu WHERE %s AND time >= '%s' AND time < '%s' AND usage_user > 90 AND usage_guest > 90 ORDER BY hostname,bucket"

	queryTemplate := "SELECT time as bucket,hostname,# FROM cpu WHERE %s AND time >= '%s' AND time < '%s'$  ORDER BY hostname,bucket"

	particalSegments := make([]string, 0)
	qryTemplates := make([]string, 0)
	for _, qf := range queryFields {
		particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{(%s>95[int64])(%s>95[int64])}#{empty,empty}", JoinSegmentFields(qf, cpu_datatype), qf[0], qf[1]))
		tmp := queryTemplate
		tmp = strings.Replace(tmp, "#", strings.Join(qf, ","), 1)
		tmp = strings.Replace(tmp, "$", fmt.Sprintf(" AND %s > 95 AND %s > 95", qf[0], qf[1]), 1)
		qryTemplates = append(qryTemplates, tmp)
		for _, ag := range tsclaggr {
			for _, in := range tsclInterval {
				flds := make([]string, 0)
				for _, f := range qf {
					flds = append(flds, fmt.Sprintf("%s(%s)", ag, f))
				}
				qryTemplates = append(qryTemplates, fmt.Sprintf("SELECT time_bucket('%d minute', time) as bucket,hostname,%s FROM cpu WHERE %%s AND time >= '%%s' AND time < '%%s' GROUP BY hostname,bucket ORDER BY hostname,bucket", in, strings.Join(flds, ",")))

				tar := ag
				if strings.EqualFold(ag, "avg") {
					ag = "mean"
				}
				particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{empty}#{%s,%dm}", JoinSegmentFields(qf, cpu_datatype), ag, in))
				ag = tar
			}
		}
	}

	return qryTemplates, particalSegments
}

func GenerateTimescaleIotQueryTemplate() ([]string, []string) {
	queryFields1 := SplitFields(tscl_iot_readings_fields)
	//queryFields2 := SplitFields(tscl_iot_diagnostics_fields)
	//"SELECT time_bucket('5 minute', time) as bucket,name,avg(latitude),avg(longitude) FROM readings WHERE %s AND time >= '%s' AND time < '%s' GROUP BY name,bucket ORDER BY name,bucket"
	//"SELECT time as bucket,name,current_load,fuel_state FROM diagnostics WHERE %s AND time >= '%s' AND time < '%s' AND fuel_state > 0.9 AND current_load > 4500 ORDER BY name,bucket"

	queryTemplate1 := "SELECT time as bucket,name,# FROM readings WHERE %s AND time >= '%s' AND time < '%s' $ ORDER BY name,bucket"
	//queryTemplate2 := "SELECT time as bucket,name,# FROM diagnostics WHERE %s AND time >= '%s' AND time < '%s'$  ORDER BY name,bucket"

	particalSegments := make([]string, 0)
	qryTemplates := make([]string, 0)
	for _, qf := range queryFields1 {
		qryP, segP := JoinTimescalePredicate(qf)
		if qryP != "" && segP != "" {
			particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{%s}#{empty,empty}", JoinSegmentFields(qf, iot_datatype), segP))
			tmp := queryTemplate1
			tmp = strings.Replace(tmp, "#", strings.Join(qf, ","), 1)
			tmp = strings.Replace(tmp, "$", qryP, 1)
			qryTemplates = append(qryTemplates, tmp)
		}
		for _, ag := range tsclaggr {
			for _, in := range tsclInterval {
				flds := make([]string, 0)
				for _, f := range qf {
					flds = append(flds, fmt.Sprintf("%s(%s)", ag, f))
				}
				qryTemplates = append(qryTemplates, fmt.Sprintf("SELECT time_bucket('%d minute', time) as bucket,name,%s FROM readings WHERE %%s AND time >= '%%s' AND time < '%%s' GROUP BY name,bucket ORDER BY name,bucket", in, strings.Join(flds, ",")))

				tar := ag
				if strings.EqualFold(ag, "avg") {
					ag = "mean"
				}
				particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{empty}#{%s,%dm}", JoinSegmentFields(qf, iot_datatype), ag, in))
				ag = tar
			}
		}
	}

	//for _, qf := range queryFields2 {
	//	particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{(%s>95[float64])(%s>95[float64])}#{empty,empty}", JoinSegmentFields(qf, iot_datatype), qf[0], qf[1]))
	//	tmp := queryTemplate2
	//	tmp = strings.Replace(tmp, "#", strings.Join(qf, ","), 1)
	//	tmp = strings.Replace(tmp, "$", fmt.Sprintf(" AND %s > 95 AND %s > 95", qf[0], qf[1]), 1)
	//	qryTemplates = append(qryTemplates, tmp)
	//	for _, ag := range tsclaggr {
	//		for _, in := range tsclInterval {
	//			flds := make([]string, 0)
	//			for _, f := range qf {
	//				flds = append(flds, fmt.Sprintf("%s(%s)", ag, f))
	//			}
	//			qryTemplates = append(qryTemplates, fmt.Sprintf("SELECT time_bucket('%d minute', time) as bucket,name,%s FROM diagnostics WHERE %%s AND time >= '%%s' AND time < '%%s' GROUP BY name,bucket ORDER BY name,bucket", in, strings.Join(flds, ",")))
	//
	//			tar := ag
	//			if strings.EqualFold(ag, "avg") {
	//				ag = "mean"
	//			}
	//			particalSegments = append(particalSegments, fmt.Sprintf("#{%s}#{empty}#{%s,%dm}", JoinSegmentFields(qf, iot_datatype), ag, in))
	//			ag = tar
	//		}
	//	}
	//}

	return qryTemplates, particalSegments
}
