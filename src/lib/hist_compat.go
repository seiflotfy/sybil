package sybil

import "strconv"

import "github.com/codahale/hdrhistogram"

// {{{ HDR HIST
type HDRHist struct {
	*hdrhistogram.Histogram

	table *Table
	info  *IntInfo

	track_percentiles bool
}

func (th *HDRHist) NewHist() Histogram {
	return th.table.NewHDRHist(th.info)
}

func (t *Table) NewHDRHist(info *IntInfo) *HDRHist {
	hdr_hist := hdrhistogram.New(info.Min, info.Max*2, 5)
	outer_hist := HDRHist{hdr_hist, t, info, true}

	return &outer_hist

}

func (th *HDRHist) Combine(oh interface{}) {
	hist := oh.(*HDRHist)
	th.Histogram.Merge(hist.Histogram)
}

func (th *HDRHist) GetVariance() float64 {
	std := th.StdDev()
	return std * std
}

func (th *HDRHist) GetPercentiles() []int64 {

	ret := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ret[i] = th.ValueAtQuantile(float64(i))
	}

	return ret
}

func (th *HDRHist) GetBuckets() map[string]int64 {
	ret := make(map[string]int64)
	for _, v := range th.Distribution() {
		key := strconv.FormatInt(int64(v.From+v.To)/2, 10)
		ret[key] = v.Count
	}

	return ret

}

// }}} HDR HIST

// {{{ HIST COMPAT WRAPPER FOR BASIC HIST

type HistCompat struct {
	*BasicHist

	Histogram *BasicHist
}

func (hc *HistCompat) Min() int64 {

	return hc.Histogram.Min
}

func (hc *HistCompat) Max() int64 {
	return hc.Histogram.Max
}

func (hc *HistCompat) NewHist() Histogram {
	return hc.table.NewHist(hc.info)
}

func (h *HistCompat) Mean() float64 {
	return h.Avg
}

func (h *HistCompat) GetMeanVariance() float64 {
	return h.GetVariance() / float64(h.Count)
}

func (h *HistCompat) TotalCount() int64 {
	return h.Count
}

func (h *HistCompat) StdDev() float64 {
	return h.GetStdDev()
}

// compat layer with hdr hist
func (h *HistCompat) RecordValues(value int64, n int64) error {
	h.addWeightedValue(value, n)

	return nil
}

func (h *HistCompat) Distribution() map[string]int64 {
	return h.GetBuckets()
}

// }}}

// {{{ HIST COMPAT WRAPPER FOR MULTI HIST

type MultiHistCompat struct {
	*MultiHist

	Histogram *MultiHist
}

func (hc *MultiHistCompat) Min() int64 {

	return hc.Histogram.Min
}

func (hc *MultiHistCompat) Max() int64 {
	return hc.Histogram.Max
}

func (hc *MultiHistCompat) NewHist() Histogram {
	return hc.table.NewMultiHist(hc.info)
}

func (h *MultiHistCompat) Mean() float64 {
	return h.Avg
}

func (h *MultiHistCompat) GetMeanVariance() float64 {
	return h.GetVariance() / float64(h.Count)
}

func (h *MultiHistCompat) TotalCount() int64 {
	return h.Count
}

func (h *MultiHistCompat) StdDev() float64 {
	return h.GetStdDev()
}

// compat layer with hdr hist
func (h *MultiHistCompat) RecordValues(value int64, n int64) error {
	h.addWeightedValue(value, n)

	return nil
}

func (h *MultiHistCompat) Distribution() map[string]int64 {
	return h.GetBuckets()
}

// }}}
