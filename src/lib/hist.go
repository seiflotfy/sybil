package sybil

var NUM_BUCKETS = 1000
var DEBUG_OUTLIERS = false

// histogram types:
// HDRHist (wrapper around github.com/codahale/hdrhistogram which implements Histogram interface)
// BasicHist (which gets wrapped in HistCompat to implement the Histogram interface)

type Histogram interface {
	Mean() float64
	Max() int64
	Min() int64
	TotalCount() int64

	RecordValues(int64, int64) error
	GetPercentiles() []int64
	GetBuckets() map[string]int64
	StdDev() float64
	NewHist() Histogram

	Combine(interface{})
}

func (t *Table) NewHist(info *IntInfo) Histogram {
	var hist Histogram
	if *FLAGS.HDR_HIST && ENABLE_HDR {
		hist = newHDRHist(t, info)
	} else if *FLAGS.LOG_HIST {
		hist = t.NewMultiHist(info)
	} else {
		hist = t.NewBasicHist(info)
	}

	return hist
}
