package kwdb_client

import (
	"context"
	"fmt"
	"testing"

	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
)

func TestConnect(t *testing.T) {
	user := "root"
	pass := ""
	host := "127.0.0.1"
	port := 26257

	db, err := commonpool.GetConnection(user, pass, host, "", port)
	if err != nil {
		panic(err)
	}

	rows, err := db.Connection.Query(context.Background(), `show databases;`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbName, engine_type string
		if err := rows.Scan(&dbName, &engine_type); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("%s\t%s\n", dbName, engine_type)
	}

}
