//+build !hdrhist

package pkg

var ENABLE_HDR = false

func newHDRHist(table *Table, info *IntInfo) Histogram {
	return table.NewHist(info)
}
