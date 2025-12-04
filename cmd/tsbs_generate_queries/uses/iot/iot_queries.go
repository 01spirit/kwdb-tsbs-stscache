package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type IoTQueries struct {
	core utils.QueryGenerator
}

func NewIoTQueries(core utils.QueryGenerator) utils.QueryFiller {
	return &IoTQueries{
		core: core,
	}
}

func (i *IoTQueries) Fill(q query.Query) query.Query {
	fc, ok := i.core.(IotQueriesFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.IotQueries(q)
	return q
}
