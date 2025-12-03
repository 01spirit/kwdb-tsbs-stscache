package SemanticGraph

import (
	"database/sql"
	//influxdb_client "github.com/timescale/tsbs/InfluxDB-client/v2"
	"sort"
	"sync"
)

type HistoryRecord struct {
	Record map[string]uint64
}

type QueryStat struct {
	StatInfo map[string]uint64
}

func NewHistoryRecord() *HistoryRecord {
	return &HistoryRecord{Record: make(map[string]uint64)}
}

func (s *HistoryRecord) SortedRecord() ([]string, []uint64) {
	keys := make([]string, len(s.Record))
	for key := range s.Record {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	nums := make([]uint64, len(s.Record))
	for _, key := range keys {
		nums = append(nums, s.Record[key])
	}
	return keys, nums
}

var RecordLock sync.Mutex

//func (s *HistoryRecord) StatInflux(queryString, segment, tableName string, conn influxdb_client.Client) {
//	qry := influxdb_client.NewQuery(queryString, tableName, "s")
//	resp, err := conn.Query(qry)
//	if err != nil {
//		panic(err)
//	}
//	totalRowLength := uint64(0)
//	for _, result := range resp.Results {
//		for _, series := range result.Series {
//			totalRowLength += uint64(len(series.Values))
//		}
//	}
//	colNum := ColumnNum(segment)
//	RecordLock.Lock()
//	s.Record[segment] += totalRowLength * uint64(colNum)
//	RecordLock.Unlock()
//	//s.Record[segment] += totalRowLength
//}

func (s *HistoryRecord) StatTimescale(queryString, segment string, conn *sql.DB) {
	rows, err := conn.Query(queryString)
	totalRowLength := uint64(0)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		totalRowLength++
	}
	err = rows.Close()
	if err != nil {
		return
	}
	colNum := ColumnNum(segment)
	RecordLock.Lock()
	s.Record[segment] += totalRowLength * uint64(colNum)
	RecordLock.Unlock()
}

func (s *HistoryRecord) StatStarSegment() *QueryStat {
	RecordLock.Lock()
	starSegments := make(map[string]uint64)
	for seg, val := range s.Record {
		//singleSegment, star, tags := SplitSegment(seg)
		_, star, _ := SplitSegment(seg)
		starSegments[star] += val
	}
	RecordLock.Unlock()
	return &QueryStat{StatInfo: starSegments}
}

func (s *HistoryRecord) AvgLoad() float64 {
	RecordLock.Lock()
	totalLoad := uint64(0)
	cnt := 0
	for _, val := range s.Record {
		totalLoad += val
		cnt++
	}
	RecordLock.Unlock()
	return float64(totalLoad) / float64(cnt)
}

func (qs *QueryStat) AvgLoad() float64 {
	totalLoad := uint64(0)
	cnt := 0
	for _, val := range qs.StatInfo {
		totalLoad += val
		cnt++
	}
	return float64(totalLoad) / float64(cnt)
}

func BuildSemanticGraph(qs *QueryStat) *SemanticGraph {
	g := NewSemanticGraph()
	for starSegment := range qs.StatInfo {
		g.BuildSemanticGraph(starSegment)
	}
	return g
}
