package dedup

import (
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// maxSamplesPerChunk is approximately the max number of samples that we may have in any given chunk.
// Please take a look at https://github.com/prometheus/tsdb/pull/397 to know where this number comes from.
// Long story short: TSDB is made in such a way, and it is made in such a way
// because you barely get any improvements in compression when the number of samples is beyond this.
// Take a look at Figure 6 in this whitepaper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
const maxSamplesPerChunk = 120

type ChunkSeries struct {
	lset labels.Labels
	chks []chunks.Meta
}

type Sample struct {
	timestamp int64
	value     float64
}

func NewSample(timestamp int64, value float64) *Sample {
	return &Sample{timestamp: timestamp, value: value}
}

type SampleIterator struct {
	samples []*Sample
	i       int
}

func NewSampleIterator(samples []*Sample) *SampleIterator {
	return &SampleIterator{samples: samples}
}

func (s *SampleIterator) Err() error {
	return nil
}

func (s *SampleIterator) At() (int64, float64) {
	return s.samples[s.i].timestamp, s.samples[s.i].value
}

func (s *SampleIterator) Next() bool {
	if s.i >= len(s.samples) {
		return false
	}
	s.i++
	return true
}

func (s *SampleIterator) Seek(t int64) bool {
	if s.i < 0 {
		s.i = 0
	}
	for {
		if s.i >= len(s.samples) {
			return false
		}
		if s.samples[s.i].timestamp >= t {
			return true
		}
		s.i++
	}
}

type SampleSeries struct {
	lset labels.Labels
	data map[SampleType][]*Sample
	res  int64
}

func NewSampleSeries(lset labels.Labels, data map[SampleType][]*Sample, res int64) *SampleSeries {
	return &SampleSeries{
		lset: lset,
		data: data,
		res:  res,
	}
}

func (ss *SampleSeries) ToChunkSeries() (*ChunkSeries, error) {
	if len(ss.data) == 0 {
		return nil, nil
	}
	if ss.res == 0 {
		return ss.toRawChunkSeries()
	}
	return ss.toDownsampleChunkSeries()
}

func (ss *SampleSeries) toRawChunkSeries() (*ChunkSeries, error) {
	chks, err := ss.toChunks(RawSample)
	if err != nil {
		return nil, err
	}
	if len(chks) == 0 {
		return nil, nil
	}
	return &ChunkSeries{
		lset: ss.lset,
		chks: chks,
	}, nil
}

func (ss *SampleSeries) toDownsampleChunkSeries() (*ChunkSeries, error) {
	all := make([][]chunks.Meta, len(downsampleAggrTypes), len(downsampleAggrTypes))
	for _, at := range downsampleAggrTypes {
		chks, err := ss.toChunks(typeMapping[at])
		if err != nil {
			return nil, err
		}
		all[at] = chks
	}
	numChks := len(all[downsample.AggrCount])
	if numChks == 0 {
		return nil, nil
	}
	result := make([]chunks.Meta, 0, numChks)
	for i := 0; i < numChks; i++ {
		var chks [5]chunkenc.Chunk
		for _, at := range downsampleAggrTypes {
			if all[at] == nil {
				continue
			}
			chks[at] = all[at][i].Chunk
		}
		result = append(result, chunks.Meta{
			MinTime: all[downsample.AggrCount][i].MinTime,
			MaxTime: all[downsample.AggrCount][i].MaxTime,
			Chunk:   downsample.EncodeAggrChunk(chks),
		})
	}

	return &ChunkSeries{
		lset: ss.lset,
		chks: result,
	}, nil
}

func (ss *SampleSeries) toChunks(st SampleType) ([]chunks.Meta, error) {
	samples := ss.data[st]
	if len(samples) == 0 {
		return nil, nil
	}
	numChks := (len(samples)-1)/maxSamplesPerChunk + 1
	chks := make([]chunks.Meta, 0, numChks)

	for i := 0; i < numChks; i++ {
		c := chunkenc.NewXORChunk()
		appender, err := c.Appender()
		if err != nil {
			return nil, err
		}
		start := i * maxSamplesPerChunk
		end := (i + 1) * maxSamplesPerChunk
		if start == len(samples) {
			break
		}
		if end > len(samples) {
			end = len(samples)
		}
		for _, v := range samples[start:end] {
			appender.Append(v.timestamp, v.value)
		}
		// InjectThanosMeta the chunk's counter aggregate with the last true sample.
		if st == CounterSample {
			appender.Append(samples[end-1].timestamp, samples[end-1].value)
		}
		chks = append(chks, chunks.Meta{
			MinTime: samples[start].timestamp,
			MaxTime: samples[end-1].timestamp,
			Chunk:   c,
		})
	}
	return chks, nil
}

type SampleReader struct {
	logger log.Logger
	cr     tsdb.ChunkReader
	lset   labels.Labels
	chks   []chunks.Meta
	res    int64
}

func NewSampleReader(logger log.Logger, cr tsdb.ChunkReader, lset labels.Labels, chks []chunks.Meta, res int64) *SampleReader {
	return &SampleReader{
		logger: logger,
		cr:     cr,
		lset:   lset,
		chks:   chks,
		res:    res,
	}
}

func (r *SampleReader) Read(tw *TimeWindow) (map[SampleType][]*Sample, error) {
	if len(r.chks) == 0 {
		return nil, nil
	}
	if r.res == 0 {
		return r.readRawSamples(tw)
	}
	return r.readDownSamples(tw)
}

func (r *SampleReader) readRawSamples(tw *TimeWindow) (map[SampleType][]*Sample, error) {
	samples := make([]*Sample, 0)
	for _, c := range r.chks {
		chk, err := r.cr.Chunk(c.Ref)
		if err != nil {
			return nil, errors.Wrapf(err, "get raw chunk %d for labels %s", c.Ref, r.lset)
		}
		if chk == nil {
			level.Warn(r.logger).Log("msg", "find empty raw chunk", "ref", c.Ref, "labels", r.lset)
			continue
		}
		ss := r.parseSamples(chk, tw)
		if len(ss) == 0 {
			continue
		}
		samples = append(samples, ss...)
	}
	if len(samples) == 0 {
		return nil, nil
	}
	result := make(map[SampleType][]*Sample)
	result[RawSample] = samples
	return result, nil
}

func (r *SampleReader) readDownSamples(tw *TimeWindow) (map[SampleType][]*Sample, error) {
	result := make(map[SampleType][]*Sample)
	result[CountSample] = make([]*Sample, 0)
	result[SumSample] = make([]*Sample, 0)
	result[MinSample] = make([]*Sample, 0)
	result[MaxSample] = make([]*Sample, 0)
	result[CounterSample] = make([]*Sample, 0)
	for _, c := range r.chks {
		chk, err := r.cr.Chunk(c.Ref)
		if err != nil {
			return nil, errors.Wrapf(err, "get downsample chunk %d for labels %s", c.Ref, r.lset)
		}
		for _, at := range downsampleAggrTypes {
			ac, err := chk.(*downsample.AggrChunk).Get(at)
			if err == downsample.ErrAggrNotExist {
				continue
			}
			if ac == nil {
				level.Warn(r.logger).Log("msg", "find empty downsample chunk", "type", at, "ref", c.Ref, "labels", r.lset)
				continue
			}
			samples := r.parseSamples(ac, tw)
			if len(samples) == 0 {
				continue
			}
			result[typeMapping[at]] = append(result[typeMapping[at]], samples...)
		}
	}
	return result, nil
}

func (r *SampleReader) parseSamples(c chunkenc.Chunk, tw *TimeWindow) []*Sample {
	samples := make([]*Sample, 0)
	iterator := c.Iterator()
	for iterator.Next() {
		timestamp, value := iterator.At()
		if timestamp < tw.MinTime {
			continue
		}
		// Ignore the data point which timestamp is same with MaxTime.
		// Make sure the block use scope [MinTime, MaxTime) instead of [MinTime, MaxTime]
		if timestamp >= tw.MaxTime {
			break
		}
		samples = append(samples, &Sample{
			timestamp: timestamp,
			value:     value,
		})
	}
	return samples
}

type SampleType uint8

const (
	// samples from non-downsample blocks
	RawSample SampleType = iota
	// samples from downsample blocks, map to downsample AggrType
	CountSample
	SumSample
	MinSample
	MaxSample
	CounterSample
)

var (
	typeMapping = map[downsample.AggrType]SampleType{
		downsample.AggrCount:   CountSample,
		downsample.AggrSum:     SumSample,
		downsample.AggrMin:     MinSample,
		downsample.AggrMax:     MaxSample,
		downsample.AggrCounter: CounterSample,
	}
	downsampleAggrTypes = []downsample.AggrType{
		downsample.AggrCount,
		downsample.AggrSum,
		downsample.AggrMin,
		downsample.AggrMax,
		downsample.AggrCounter,
	}
)

func (t SampleType) String() string {
	switch t {
	case RawSample:
		return "raw"
	case CountSample:
		return "count"
	case SumSample:
		return "sum"
	case MinSample:
		return "min"
	case MaxSample:
		return "max"
	case CounterSample:
		return "counter"
	}
	return "<unknown>"
}

type BlockReader struct {
	logger  log.Logger
	closers []io.Closer

	ir tsdb.IndexReader
	cr tsdb.ChunkReader

	postings index.Postings
}

func NewBlockReader(logger log.Logger, resolution int64, blockDir string) (*BlockReader, error) {
	reader := &BlockReader{
		logger:  logger,
		closers: make([]io.Closer, 0, 3),
	}

	var pool chunkenc.Pool
	if resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = downsample.NewPool()
	}

	b, err := tsdb.OpenBlock(logger, blockDir, pool)
	if err != nil {
		return reader, errors.Wrapf(err, "open block under dir %s", blockDir)
	}
	reader.closers = append(reader.closers, b)

	ir, err := b.Index()
	if err != nil {
		return reader, errors.Wrap(err, "open index")
	}
	reader.ir = ir
	reader.closers = append(reader.closers, ir)

	cr, err := b.Chunks()
	if err != nil {
		return reader, errors.Wrap(err, "open chunks")
	}
	reader.cr = cr
	reader.closers = append(reader.closers, cr)

	postings, err := ir.Postings(index.AllPostingsKey())
	if err != nil {
		return reader, errors.Wrap(err, "read index postings")
	}
	reader.postings = ir.SortedPostings(postings)

	return reader, nil
}

func (r *BlockReader) Symbols() (map[string]struct{}, error) {
	return r.ir.Symbols()
}

func (r *BlockReader) Close() error {
	var merr tsdb.MultiError
	for i := len(r.closers) - 1; i >= 0; i-- {
		merr.Add(r.closers[i].Close())
	}
	return errors.Wrap(merr.Err(), "close closers")
}
