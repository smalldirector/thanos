package dedup

import (
	"context"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/prometheus/tsdb/chunkenc"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestSampleSeries_ToChunkSeries(t *testing.T) {
	rawData := make(map[SampleType][]*Sample)
	for i := 0; i < maxSamplesPerChunk; i++ {
		rawData[RawSample] = append(rawData[RawSample], &Sample{timestamp: int64(i), value: rand.Float64()})
	}

	downsampleData := make(map[SampleType][]*Sample)
	for i := 0; i < maxSamplesPerChunk+1; i++ {
		downsampleData[CountSample] = append(downsampleData[CountSample], &Sample{timestamp: int64(i), value: rand.Float64()})
		downsampleData[SumSample] = append(downsampleData[SumSample], &Sample{timestamp: int64(i), value: rand.Float64()})
		downsampleData[MinSample] = append(downsampleData[MinSample], &Sample{timestamp: int64(i), value: rand.Float64()})
		downsampleData[MaxSample] = append(downsampleData[MaxSample], &Sample{timestamp: int64(i), value: rand.Float64()})
		downsampleData[CounterSample] = append(downsampleData[CounterSample], &Sample{timestamp: int64(i), value: rand.Float64()})
	}

	lset := labels.Labels{
		{Name: "b", Value: "1"},
		{Name: "a", Value: "1"},
	}

	input := []struct {
		series *SampleSeries
	}{
		{
			&SampleSeries{
				lset: lset,
				data: rawData,
				res:  0,
			},
		},
		{
			&SampleSeries{
				lset: lset,
				data: downsampleData,
				res:  300000,
			},
		},
	}

	for _, v := range input {
		cs, err := v.series.ToChunkSeries()
		testutil.Ok(t, err)
		if v.series.res == 0 {
			testutil.Assert(t, len(cs.chks) == 1, "chunk series conversion failed")
			for _, chk := range cs.chks {
				_, ok := chk.Chunk.(*chunkenc.XORChunk)
				testutil.Equals(t, true, ok, "chunk series conversion failed")
			}
		} else {
			testutil.Assert(t, len(cs.chks) == 2, "chunk series conversion failed")
			for _, chk := range cs.chks {
				_, ok := chk.Chunk.(*downsample.AggrChunk)
				testutil.Equals(t, true, ok, "chunk series conversion failed")
			}
		}
	}
}

func TestNewBlockReader(t *testing.T) {
	reader := createBlockReader(t)
	testutil.Assert(t, reader != nil, "new block reader failed")
	testutil.Assert(t, reader.ir != nil, "new block reader failed")
	testutil.Assert(t, reader.cr != nil, "new block reader failed")
	testutil.Assert(t, reader.postings != nil, "new block reader failed")
	testutil.Assert(t, len(reader.closers) == 3, "new block reader failed")
}

func TestBlockReader_Symbols(t *testing.T) {
	reader := createBlockReader(t)
	symbols, err := reader.Symbols()
	testutil.Ok(t, err)
	testutil.Assert(t, len(symbols) > 0, "new block reader failed")
}

func TestBlockReader_Close(t *testing.T) {
	reader := createBlockReader(t)
	err := reader.Close()
	testutil.Ok(t, err)
}

func createBlockReader(t *testing.T) *BlockReader {
	dataDir, err := ioutil.TempDir("", "thanos-dedup-streamed-block-reader")
	testutil.Ok(t, err)
	id := createBlock(t, context.Background(), dataDir, "r0")

	blockDir := filepath.Join(dataDir, id.String())

	reader, err := NewBlockReader(log.NewNopLogger(), 0, blockDir)
	testutil.Ok(t, err)

	return reader
}
