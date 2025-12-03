package SemanticGraph

import (
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	//influxdb_client "github.com/timescale/tsbs/InfluxDB-client/v2"
	"testing"
)

//func TestQueryStatInflux(t *testing.T) {
//	var conn, _ = influxdb_client.NewHTTPClient(influxdb_client.HTTPConfig{
//		Addr: "http://192.168.1.101:8086",
//	})
//	tableName := "devops_small"
//	record := NewHistoryRecord()
//
//	queryString1 := "SELECT mean(usage_system),mean(usage_idle),mean(usage_nice) FROM \"cpu\" WHERE (hostname = 'host_1' or hostname = 'host_2' or hostname = 'host_3') AND TIME >= '2022-12-30T09:00:00Z' AND TIME < '2022-12-30T10:00:00Z' GROUP BY \"hostname\",time(15m)"
//	segment1 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"
//
//	queryString2 := "SELECT mean(usage_system),mean(usage_idle),mean(usage_nice) FROM \"cpu\" WHERE (hostname = 'host_4' or hostname = 'host_5' or hostname = 'host_6') AND TIME >= '2022-12-30T08:00:00Z' AND TIME < '2022-12-30T10:00:00Z' GROUP BY \"hostname\",time(15m)"
//	segment2 := "{(cpu.hostname=host_4)(cpu.hostname=host_5)(cpu.hostname=host_6)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"
//
//	record.StatInflux(queryString1, segment1, tableName, conn)
//	record.StatInflux(queryString2, segment2, tableName, conn)
//
//	for k, v := range record.Record {
//		fmt.Println(k, " : ", v)
//	}
//
//	fmt.Println()
//	stat := record.StatStarSegment()
//	for k, v := range stat.StatInfo {
//		fmt.Println(k, " : ", v)
//	}
//}

const pgxDriver string = "pgx"

func TestQueryStatTimescale(t *testing.T) {
	// 必须 import (_ "github.com/jackc/pgx/v4/stdlib") 才能用
	var conn, err = sql.Open(pgxDriver, "host=192.168.1.101 dbname=devops_small user=postgres   sslmode=disable port=5432 password=Dell@123")
	if err != nil {
		panic(err)
	}
	record := NewHistoryRecord()

	queryString := "SELECT time_bucket('5 minute', time) as bucket,name,avg(velocity),avg(fuel_consumption) FROM diagnostics WHERE name IN ('truck_82','truck_99','truck_1','truck_86','truck_57','truck_7','truck_42','truck_60','truck_73','truck_80') AND time >= '2022-12-24 14:30:00 +0000' AND time < '2022-12-24 15:00:00 +0000' GROUP BY name,bucket ORDER BY name,bucket;"
	_, err = conn.Query(queryString)
	if err != nil {
		panic(err)
	}

	queryString1 := "SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_system),avg(usage_idle),avg(usage_nice) FROM cpu WHERE hostname IN ('host_1','host_2','host_3') AND time >= '2022-12-30 09:00:00 +0000' AND time < '2022-12-30 10:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket;"
	segment1 := "{(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"

	queryString2 := "SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_system),avg(usage_idle),avg(usage_nice) FROM cpu WHERE hostname IN ('host_4','host_5','host_6') AND time >= '2022-12-30 08:00:00 +0000' AND time < '2022-12-30 10:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket;"
	segment2 := "{(cpu.hostname=host_4)(cpu.hostname=host_5)(cpu.hostname=host_6)}#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,15m}"

	record.StatTimescale(queryString1, segment1, conn)
	record.StatTimescale(queryString2, segment2, conn)

	for k, v := range record.Record {
		fmt.Println(k, " : ", v)
	}

	fmt.Println()
	stat := record.StatStarSegment()
	for k, v := range stat.StatInfo {
		fmt.Println(k, " : ", v)
	}

	g := BuildSemanticGraph(stat)
	g.PrintGraph()
}
