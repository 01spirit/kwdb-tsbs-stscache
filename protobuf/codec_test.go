package codec

import (
	"fmt"
	"log"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestCodec(t *testing.T) {
	// 创建一个 SemanticMeta 消息实例
	meta := &SemanticMeta{
		MetaSegment: "example_meta",
		SegmentLen:  10,
		SeriesNum:   2,
		SeriesArr: []*SemanticSeries{
			{
				SeriesSegment: "series1",
				SegmentLen:    5,
				Value:         []byte{1, 2, 3, 4, 5},
			},
			{
				SeriesSegment: "series2",
				SegmentLen:    3,
				Value:         []byte{6, 7, 8},
			},
		},
	}

	// 序列化为二进制数据
	data, err := proto.Marshal(meta)
	if err != nil {
		log.Fatalf("Failed to marshal SemanticMeta: %v", err)
	}
	fmt.Printf("Serialized data: %v\n", data)

	// 反序列化
	var newMeta SemanticMeta
	if err := proto.Unmarshal(data, &newMeta); err != nil {
		log.Fatalf("Failed to unmarshal SemanticMeta: %v", err)
	}
	fmt.Printf("Deserialized SemanticMeta: %+v\n", newMeta)
}
