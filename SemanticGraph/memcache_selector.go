package SemanticGraph

import (
	"hash/crc32"
	"log"
	"sync"
)

var SelectorMap = make(map[string][]uint64)

var keyBufPool = sync.Pool{New: func() interface{} {
	b := make([]byte, 25600)
	return &b
}}

func GetSelectorPostingList() []map[string]uint64 {
	res := make([]map[string]uint64, 0)
	for i := 0; i < len(CInstanceSlice); i++ {
		m := make(map[string]uint64)
		for seg, val := range SelectorMap {
			if int(val[1]) == i {
				m[seg] = val[0]
			}
		}
		res = append(res, m)
	}
	return res
}

func SelectorAvgLoad(stat map[string]uint64) float64 {
	totalLoad := uint64(0)
	cnt := 0
	for _, val := range stat {
		totalLoad += val
		cnt++
	}
	return float64(totalLoad) / float64(cnt)
}

func PickServer(key string) (idx int) {
	if len(CInstanceSlice) == 0 {
		log.Fatal("no instance server to allocate")
	}
	if len(CInstanceSlice) == 1 {
		return 0
	}
	bufp := keyBufPool.Get().(*[]byte)
	n := copy(*bufp, key)
	cs := crc32.ChecksumIEEE((*bufp)[:n])
	keyBufPool.Put(bufp)
	return int(cs) % len(CInstanceSlice)
}

func BuildSelectorMap(qs map[string]uint64) {
	RecordLock.Lock()
	for seg, load := range qs {
		SelectorMap[seg] = []uint64{load, uint64(PickServer(seg))}
	}
	RecordLock.Unlock()
}
