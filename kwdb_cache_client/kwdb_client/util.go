package kwdb_client

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

const goTimeFmt = "2006-01-02 15:04:05.999999 -0700"

func ResponseIsEmpty(rows pgx.Rows) bool {
	if rows == nil {
		return true
	}
	if rows.Err() != nil {
		return true
	}

	return false
}

func GetQueryTemplate(queryString string) (string, int64, int64, []string) {
	var startTime int64
	var endTime int64
	var tags []string

	//timeReg := regexp.MustCompile(`\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s[+-]\d{4}`)
	timeReg := regexp.MustCompile(`\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\s*[+-]\d{2}:?\d{2}`)

	times := timeReg.FindAllString(queryString, -1)
	if len(times) == 0 {
		startTime = 0
		endTime = 0
	} else if len(times) == 1 {
		startTime = TimeStringToInt64(times[0])
		endTime = startTime
	} else {
		startTime = TimeStringToInt64(times[0])
		endTime = TimeStringToInt64(times[1])
	}

	hostRe := regexp.MustCompile(`(?i)hostname\s+IN\s*\(([^)]+)\)`)
	m := hostRe.FindStringSubmatch(queryString)
	if len(m) > 1 {
		// 去掉引号、空格，按逗号拆
		s := strings.ReplaceAll(m[1], `"`, "")
		s = strings.ReplaceAll(s, `'`, "")
		s = strings.TrimSpace(s)
		for _, h := range strings.Split(s, ",") {
			h = strings.TrimSpace(h)
			if h != "" {
				tags = append(tags, h)
			}
		}
		sort.Strings(tags)
	}

	// 3. 生成模板：把具体时间、具体 hosts 换成 ?
	tpl := timeReg.ReplaceAllString(queryString, "?")
	tpl = hostRe.ReplaceAllString(tpl, "hostname IN (?)")

	for i := range tags {
		tags[i] = fmt.Sprintf("hostname=%s", tags[i])
	}

	return tpl, startTime, endTime, tags
}

func EmptyResultByteArray(segment string) []byte {
	emptyValues := make([]byte, 0)
	zero, _ := Int64ToByteArray(int64(0))
	emptyValues = append(emptyValues, []byte(segment)...)
	emptyValues = append(emptyValues, []byte(" ")...)
	emptyValues = append(emptyValues, zero...)

	return emptyValues
}

func RowsToInterface(rows pgx.Rows, colLen int) [][]interface{} {
	if rows == nil {
		return nil
	}
	if rows.Err() != nil {
		return nil
	}
	results := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, colLen)
		for i := range values {
			values[i] = new(interface{})
		}
		err := rows.Scan(values...)
		if err != nil {
			panic(errors.Wrap(err, "error while reading values"))
		}

		data := make([]interface{}, 0)
		for i := range values {
			data = append(data, *values[i].(*interface{}))
		}

		results = append(results, data)
	}

	rows.Close()

	return results
}

func DataTypeFromColumn(colLen int) []string {
	if colLen <= 2 {
		return nil
	}

	results := make([]string, 0)

	results = append(results, "int64")
	results = append(results, "string")

	for i := 0; i < colLen-2; i++ {
		results = append(results, "float64")
	}

	return results
}

func GetDataTypeArrayFromSF(sfString string) []string {
	datatypes := make([]string, 0)
	columns := strings.Split(sfString, ",")

	for _, col := range columns {
		startIdx := strings.Index(col, "[") + 1
		endIdx := strings.Index(col, "]")
		datatypes = append(datatypes, col[startIdx:endIdx])
	}

	return datatypes
}

func TimeInt64ToString(number int64) string {
	layout := goTimeFmt
	t := time.Unix(number, 0)
	timestamp := t.Format(layout)

	return timestamp
}

func TimeStringToInt64(timestamp string) int64 {
	layout := goTimeFmt
	timeT, _ := time.Parse(layout, timestamp)
	numberN := timeT.Unix()

	return numberN
}
