package kwdb_client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/tsbs/SemanticGraph"
	stscache "github.com/timescale/tsbs/kwdb_cache_client/stscache_client"
)

//const pgxDriver = "pgx"

var DB = "devops_small"
var DbName = ""

var STsCacheURL string

var UseCache = "db"

var MaxThreadNum = 64

const STRINGBYTELENGTH = 32

var CacheHash = make(map[string]int)

func GetCacheHashValue(fields string) int {
	CacheNum := len(STsConnArr)
	if CacheNum == 0 {
		CacheNum = 1
	}
	if _, ok := CacheHash[fields]; !ok {
		value := len(CacheHash) % CacheNum
		CacheHash[fields] = value
	}
	hashValue := CacheHash[fields]
	return hashValue
}

var mtx sync.Mutex

var STsConnArr []*stscache.Client

func InitStsConnsArr(urlArr []string) []*stscache.Client {
	conns := make([]*stscache.Client, 0)
	for i := 0; i < len(urlArr); i++ {
		conns = append(conns, stscache.New(urlArr[i]))
	}
	return conns
}

func STsCacheClientSeg(conn *pgx.Conn, queryString string, semanticSegment string) ([][][]interface{}, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString)

	partialSegment := ""
	fields := ""
	metric := ""
	partialSegment, fields, metric = SplitPartialSegment(semanticSegment)

	starSegment := GetStarSegment(metric, partialSegment)

	CacheIndex := 0
	fields = "time[int64],name[string]," + fields
	colLen := strings.Split(fields, ",")
	datatypes := DataTypeFromColumn(len(colLen))

	values, _, err := STsConnArr[CacheIndex].Get(semanticSegment, startTime, endTime)
	if err != nil {
		//fmt.Println(err)
		rows, err := conn.Query(context.Background(), queryString)
		if err != nil {
			log.Println(queryString)
		}

		var dataArray [][]interface{} = nil
		if !ResponseIsEmpty(rows) {
			dataArray = RowsToInterface(rows, len(datatypes))
			remainValues, _ := ResponseInterfaceToByteArrayWithParams(dataArray, datatypes, tags, metric, partialSegment)
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Time_start: startTime, Time_end: endTime, Value: remainValues})
		} else {
			singleSemanticSegment := GetSingleSegment(metric, partialSegment, tags)
			emptyValues := InsertEmptyBytes(singleSemanticSegment)
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Time_start: startTime, Time_end: endTime, Value: emptyValues})
		}

		return [][][]interface{}{dataArray}, byteLength, hitKind

	} else {
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)
		if flagNum == 0 {
			hitKind = 2
			return convertedResponse, byteLength, hitKind
		} else {
			hitKind = 1
			remainQueryString, minTime, maxTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
			remainTags := make([]string, 0)
			for i, tag := range tagArr {
				if flagArr[i] == 1 {
					remainTags = append(remainTags, fmt.Sprintf("%s=%s", tag[0], tag[1]))
				}
			}
			if maxTime-minTime <= int64(time.Minute.Seconds()) {
				hitKind = 2

				return convertedResponse, byteLength, hitKind
			}
			remainRows, err := conn.Query(context.Background(), remainQueryString)
			if err != nil {
				log.Println(remainQueryString)
				log.Println(err)
			}
			if ResponseIsEmpty(remainRows) {
				hitKind = 2
				singleSemanticSegment := GetSingleSegment(metric, partialSegment, remainTags)
				emptyValues := InsertEmptyBytes(singleSemanticSegment)
				err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Time_start: startTime, Time_end: endTime, Value: emptyValues})
				return convertedResponse, byteLength, hitKind
			}

			remainDataArray := RowsToInterface(remainRows, len(datatypes))
			remainByteArr, _ := ResponseInterfaceToByteArrayWithParams(remainDataArray, datatypes, remainTags, metric, partialSegment)
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Time_start: minTime, Time_end: maxTime, Value: remainByteArr})
			totalResp := MergeRemainResponse(convertedResponse, remainDataArray, timeRangeArr)
			return totalResp, byteLength, hitKind
		}

	}

}

func STsCacheClientSegDistributed(instanceID []uint32, conn *pgx.Conn, queryString string, semanticSegment string) ([][][]interface{}, uint64, uint8) {
	byteLength := uint64(0)
	hitKind := uint8(0)

	idx := 0
	if len(instanceID) > 1 {
		idx = rand.Intn(len(instanceID))
	}

	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString)

	partialSegment := ""
	fields := ""
	metric := ""
	partialSegment, fields, metric = SplitPartialSegment(semanticSegment)

	starSegment := GetStarSegment(metric, partialSegment)

	fields = "time[int64],name[string]," + fields
	colLen := strings.Split(fields, ",")
	datatypes := DataTypeFromColumn(len(colLen))

	values, _, err := SemanticGraph.CInstanceSlice[instanceID[idx]].Conn.Get(semanticSegment, startTime, endTime)
	if err != nil {
		rows, err := conn.Query(context.Background(), queryString)
		if err != nil {
			log.Println(queryString)
		}

		var dataArray [][]interface{} = nil
		if !ResponseIsEmpty(rows) {
			dataArray = RowsToInterface(rows, len(datatypes))
			remainValues, _ := ResponseInterfaceToByteArrayWithParams(dataArray, datatypes, tags, metric, partialSegment)
			for _, id := range instanceID {
				go SemanticGraph.CInstanceSlice[id].Conn.Set(&stscache.Item{Key: starSegment, Time_start: startTime, Time_end: endTime, Value: remainValues})
			}

		} else {
			singleSemanticSegment := GetSingleSegment(metric, partialSegment, tags)
			emptyValues := InsertEmptyBytes(singleSemanticSegment)
			for _, id := range instanceID {
				go SemanticGraph.CInstanceSlice[id].Conn.Set(&stscache.Item{Key: starSegment, Time_start: startTime, Time_end: endTime, Value: emptyValues})
			}

		}

		return [][][]interface{}{dataArray}, byteLength, hitKind

	} else {
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)
		if flagNum == 0 {
			hitKind = 2
			return convertedResponse, byteLength, hitKind
		} else {
			hitKind = 1
			remainQueryString, minTime, maxTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
			remainTags := make([]string, 0)
			for i, tag := range tagArr {
				if flagArr[i] == 1 {
					remainTags = append(remainTags, fmt.Sprintf("%s=%s", tag[0], tag[1]))
				}
			}
			if maxTime-minTime <= int64(time.Minute.Seconds()) {
				hitKind = 2

				return convertedResponse, byteLength, hitKind
			}
			remainRows, err := conn.Query(context.Background(), remainQueryString)
			if err != nil {
				log.Println(remainQueryString)
			}
			if ResponseIsEmpty(remainRows) {
				hitKind = 2
				singleSemanticSegment := GetSingleSegment(metric, partialSegment, remainTags)
				emptyValues := InsertEmptyBytes(singleSemanticSegment)
				for _, id := range instanceID {
					go SemanticGraph.CInstanceSlice[id].Conn.Set(&stscache.Item{Key: starSegment, Time_start: startTime, Time_end: endTime, Value: emptyValues})
				}

				return convertedResponse, byteLength, hitKind
			}

			remainDataArray := RowsToInterface(remainRows, len(datatypes))
			remainByteArr, _ := ResponseInterfaceToByteArrayWithParams(remainDataArray, datatypes, remainTags, metric, partialSegment)
			for _, id := range instanceID {
				go SemanticGraph.CInstanceSlice[id].Conn.Set(&stscache.Item{Key: starSegment, Time_start: minTime, Time_end: maxTime, Value: remainByteArr})
			}

			totalResp := MergeRemainResponse(convertedResponse, remainDataArray, timeRangeArr)
			return totalResp, byteLength, hitKind
		}

	}

}
