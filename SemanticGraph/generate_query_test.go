package SemanticGraph

import (
	"fmt"
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestJoinPredicate(t *testing.T) {
	qf := iot_readings_fields
	qry, seg := JoinInfluxPredicate(qf)
	fmt.Println(qry)
	fmt.Println(seg)
	qry, seg = JoinTimescalePredicate(qf)
	fmt.Println(qry)
	fmt.Println(seg)
}

func TestGenerateDevopsSegment(t *testing.T) {
	//queryFields := SplitFields(cpu_fields)
	//fmt.Println(len(queryFields))
	//segments := GeneratePartSegment(queryFields, cpu_datatype)
	segments := GenerateCpuSegment()
	fmt.Println(len(segments))
	for _, seg := range segments {
		fmt.Println(seg)
	}
}

func TestGenerateIotReadingsSegment(t *testing.T) {
	segments := GenerateIotReadingsSegment()
	fmt.Println(len(segments))
	for _, seg := range segments {
		fmt.Println(seg)
	}
}

func TestGenerateIotDiagSegment(t *testing.T) {
	segments := GenerateIotDiagSegment()
	fmt.Println(len(segments))
	for _, seg := range segments {
		fmt.Println(seg)
	}
}

func TestGenerateInfluxCpuQueryTemplate(t *testing.T) {
	qrys, segs := GenerateInfluxCpuQueryTemplate()
	assert.Equal(t, len(qrys), len(segs))
	for i := range qrys {
		fmt.Println(qrys[i])
		fmt.Println(segs[i])
	}
}

func TestGenerateInfluxIotQueryTemplate(t *testing.T) {
	qrys, segs := GenerateInfluxIotQueryTemplate()
	assert.Equal(t, len(qrys), len(segs))
	for i := range qrys {
		fmt.Println(qrys[i])
		fmt.Println(segs[i])
	}
}

func TestGenerateTimescaleCpuQueryTemplate(t *testing.T) {
	qrys, segs := GenerateTimescaleCpuQueryTemplate()
	assert.Equal(t, len(qrys), len(segs))
	for i := range qrys {
		fmt.Println(qrys[i])
		fmt.Println(segs[i])
	}
}

func TestGenerateTimescaleIotQueryTemplate(t *testing.T) {
	qrys, segs := GenerateTimescaleIotQueryTemplate()
	assert.Equal(t, len(qrys), len(segs))
	for i := range qrys {
		fmt.Println(qrys[i])
		fmt.Println(segs[i])
	}
}
