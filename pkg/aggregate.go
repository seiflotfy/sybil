package pkg

import "time"
import "bytes"
import "sort"
import "strconv"
import "sync"
import "math"

import "encoding/binary"

var (
	internalResultLimit = 100000
	groupByWidth        = 8 // bytes
	groupDelimiter      = "\t"
)

const (
	noOp       = iota
	opAvg      = iota
	opHist     = iota
	opDistinct = iota
)

type sortResultsByCol struct {
	Results []*Result
	Col     string
}

func (a sortResultsByCol) Len() int      { return len(a.Results) }
func (a sortResultsByCol) Swap(i, j int) { a.Results[i], a.Results[j] = a.Results[j], a.Results[i] }

// This sorts the records in descending order
func (a sortResultsByCol) Less(i, j int) bool {
	if a.Col == Opts.SortCount {
		t1 := a.Results[i].Count
		t2 := a.Results[j].Count

		return t1 > t2
	}

	if *Flags.Op == "hist" {
		t1 := a.Results[i].Hists[a.Col].Mean()
		t2 := a.Results[j].Hists[a.Col].Mean()
		return t1 > t2

	}

	t1 := a.Results[i].Hists[a.Col].Mean()
	t2 := a.Results[j].Hists[a.Col].Mean()
	return t1 > t2
}

func filterAndAggRecords(querySpec *QuerySpec, recordsPtr *recordList) int {
	var ok bool
	binarybuffer := make([]byte, groupByWidth*len(querySpec.Groups))

	bs := make([]byte, groupByWidth)
	zero := make([]byte, groupByWidth)
	records := *recordsPtr

	var weight = int64(1)

	matchedRecords := 0
	if HoldMatches {
		querySpec.Matched = make(recordList, 0)
	}

	var resultMap ResultMap
	length := len(querySpec.Table.KeyTable)
	columns := make([]*TableColumn, length)

	if querySpec.TimeBucket <= 0 {
		resultMap = querySpec.Results
	}

	for i := 0; i < len(records); i++ {
		add := true
		r := records[i]

		if Opts.WeightCol && r.Populated[Opts.WeightColID] == INT_VAL {
			weight = int64(r.Ints[Opts.WeightColID])
		}

		// FILTERING
		for j := 0; j < len(querySpec.Filters); j++ {
			// returns True if the record matches!
			ret := querySpec.Filters[j].Filter(r) != true
			if ret {
				add = false
				break
			}
		}

		if !add {
			continue
		}

		matchedRecords++
		if HoldMatches {
			querySpec.Matched = append(querySpec.Matched, r)
		}

		if *Flags.LUA {
			continue
		}

		for i, g := range querySpec.Groups {
			copy(bs, zero)

			if columns[g.name_id] == nil && r.Populated[g.name_id] != _NO_VAL {
				columns[g.name_id] = r.block.GetColumnInfo(g.name_id)
				columns[g.name_id].Type = r.Populated[g.name_id]
			}

			switch r.Populated[g.name_id] {
			case INT_VAL:
				binary.LittleEndian.PutUint64(bs, uint64(r.Ints[g.name_id]))
			case STR_VAL:
				binary.LittleEndian.PutUint64(bs, uint64(r.Strs[g.name_id]))
			case _NO_VAL:
				binary.LittleEndian.PutUint64(bs, math.MaxUint64)
			}

			copy(binarybuffer[i*groupByWidth:], bs)
		}

		// IF WE ARE DOING A TIME SERIES AGGREGATION (WHICH CAN BE SLOWER)
		if querySpec.TimeBucket > 0 {
			if len(r.Populated) <= int(Opts.TimeColID) {
				continue
			}

			if r.Populated[Opts.TimeColID] != INT_VAL {
				continue
			}
			val := int64(r.Ints[Opts.TimeColID])

			bigRecord, bOk := querySpec.Results[string(binarybuffer)]
			if !bOk {
				if len(querySpec.Results) < internalResultLimit {
					bigRecord = NewResult()
					bigRecord.BinaryByKey = string(binarybuffer)
					querySpec.Results[string(binarybuffer)] = bigRecord
					bOk = true
				}
			}

			if bOk {
				bigRecord.Samples++
				bigRecord.Count += weight
			}

			val = int64(int(val) / querySpec.TimeBucket * querySpec.TimeBucket)
			resultMap, ok = querySpec.TimeResults[int(val)]

			if !ok {
				// TODO: this make call is kind of slow...
				resultMap = make(ResultMap)
				querySpec.TimeResults[int(val)] = resultMap
			}

		}

		addedRecord, ok := resultMap[string(binarybuffer)]

		// BUILD GROUPING RECORD
		if !ok {
			// TODO: take into account whether we are doint time series or not...
			if len(resultMap) >= internalResultLimit {
				continue
			}

			addedRecord = NewResult()
			addedRecord.BinaryByKey = string(binarybuffer)

			resultMap[string(binarybuffer)] = addedRecord
		}

		addedRecord.Samples++
		addedRecord.Count += weight

		// GO THROUGH AGGREGATIONS AND REALIZE THEM
		for _, a := range querySpec.Aggregations {
			switch r.Populated[a.name_id] {
			case INT_VAL:
				val := int64(r.Ints[a.name_id])

				hist, ok := addedRecord.Hists[a.Name]

				if !ok {
					hist = r.block.table.NewHist(r.block.table.get_int_info(a.name_id))
					addedRecord.Hists[a.Name] = hist
				}

				hist.RecordValues(val, weight)
			}

		}

	}

	// Now to unpack the byte buffers we oh so stupidly used in the group by...

	if len(querySpec.TimeResults) > 0 {
		for k, resultMap := range querySpec.TimeResults {
			querySpec.TimeResults[k] = *translateGroupBy(resultMap, querySpec.Groups, columns)
		}

	}

	if len(querySpec.Results) > 0 {
		querySpec.Results = *translateGroupBy(querySpec.Results, querySpec.Groups, columns)
	}

	if *Flags.LUA {
		querySpec.luaInit()
		querySpec.luaMap(&querySpec.Matched)
	}

	return matchedRecords

}

func translateGroupBy(Results ResultMap, Groups []Grouping, columns []*TableColumn) *ResultMap {

	var buffer bytes.Buffer

	var newResults = make(ResultMap)
	var bs []byte

	for _, r := range Results {
		buffer.Reset()
		if len(Groups) == 0 {
			buffer.WriteString("total")
		}
		for i, g := range Groups {
			bs = []byte(r.BinaryByKey[i*groupByWidth : (i+1)*groupByWidth])

			col := columns[g.name_id]

			if col == nil {
				buffer.WriteString(groupDelimiter)
				continue
			}

			val := binary.LittleEndian.Uint64(bs)
			switch col.Type {
			case INT_VAL:
				buffer.WriteString(strconv.FormatInt(int64(val), 10))
			case STR_VAL:
				buffer.WriteString(col.get_string_for_val(int32(val)))

			}

			buffer.WriteString(groupDelimiter)

		}

		r.GroupByKey = buffer.String()
		newResults[r.GroupByKey] = r
	}

	return &newResults
}

func copyQuerySpec(querySpec *QuerySpec) *QuerySpec {
	blockQuery := QuerySpec{}
	blockQuery.Table = querySpec.Table
	blockQuery.Punctuate()
	blockQuery.TimeBucket = querySpec.TimeBucket
	blockQuery.Filters = querySpec.Filters
	blockQuery.Aggregations = querySpec.Aggregations
	blockQuery.Groups = querySpec.Groups

	return &blockQuery
}

func combineMatches(blockSpecs map[string]*QuerySpec) recordList {
	start := time.Now()
	matched := make(recordList, 0)
	for _, spec := range blockSpecs {
		matched = append(matched, spec.Matched...)
	}
	end := time.Now()

	Debug("JOINING", len(matched), "MATCHED RECORDS TOOK", end.Sub(start))
	return matched

}

func combineResults(querySpec *QuerySpec, blockSpecs map[string]*QuerySpec) *QuerySpec {

	astart := time.Now()
	resultSpec := QuerySpec{}
	resultSpec.Table = querySpec.Table
	resultSpec.LuaResult = make(LuaTable, 0)

	if *Flags.LUA {
		resultSpec.luaInit()
	}

	masterResult := make(ResultMap)
	masterTimeResult := make(map[int]ResultMap)

	cumulativeResult := NewResult()
	cumulativeResult.GroupByKey = "TOTAL"
	if len(querySpec.Groups) > 1 {
		for _ = range querySpec.Groups[1:] {
			cumulativeResult.GroupByKey += "\t"
		}
	}

	for _, spec := range blockSpecs {
		masterResult.Combine(&spec.Results)

		if *Flags.LUA {
			resultSpec.luaCombine(spec)
		}

		for _, result := range spec.Results {
			cumulativeResult.Combine(result)
		}

		for i, v := range spec.TimeResults {
			mval, ok := masterTimeResult[i]

			if !ok {
				masterTimeResult[i] = v
			} else {
				for k, r := range v {
					mh, ok := mval[k]
					if ok {
						mh.Combine(r)
					} else {
						mval[k] = r
					}
				}
			}
		}
	}

	resultSpec.Cumulative = cumulativeResult
	resultSpec.TimeBucket = querySpec.TimeBucket
	resultSpec.TimeResults = masterTimeResult
	resultSpec.Results = masterResult

	if *Flags.LUA {
		resultSpec.luaFinalize()
	}

	aend := time.Now()
	Debug("AGGREGATING", len(blockSpecs), "BLOCK RESULTS TOOK", aend.Sub(astart))

	return &resultSpec
}

func sortResults(querySpec *QuerySpec) {
	// SORT THE RESULTS
	if querySpec.OrderBy != "" {
		start := time.Now()
		sorter := sortResultsByCol{}
		sorter.Results = make([]*Result, 0)
		for _, v := range querySpec.Results {
			sorter.Results = append(sorter.Results, v)
		}
		querySpec.Sorted = sorter.Results

		sorter.Col = querySpec.OrderBy
		sort.Sort(sorter)

		end := time.Now()
		if debugTiming {
			Debug("SORTING TOOK", end.Sub(start))
		}

		if len(sorter.Results) > *Flags.Limit {
			sorter.Results = sorter.Results[:*Flags.Limit]
		}

		querySpec.Sorted = sorter.Results
	}

}

func searchBlocks(querySpec *QuerySpec, blockList map[string]*TableBlock) map[string]*QuerySpec {
	var wg sync.WaitGroup
	// Each block gets its own querySpec (for locking and combining purposes)
	// after all queries finish executing, the specs are combined
	blockSpecs := make(map[string]*QuerySpec, len(blockList))

	// TODO: why iterate through blocklist after loading it instead of filtering
	// and aggregating while loading them? (and then releasing the blocks)
	// That would mean pushing the call to 'filterAndAggRecords' to the loading area
	specLock := sync.Mutex{}
	for _, block := range blockList {
		wg.Add(1)
		thisBlock := block
		go func() {
			defer wg.Done()

			blockQuery := copyQuerySpec(querySpec)

			filterAndAggRecords(blockQuery, &thisBlock.recordList)

			specLock.Lock()
			blockSpecs[thisBlock.Name] = blockQuery
			specLock.Unlock()

		}()
	}

	wg.Wait()

	return blockSpecs
}

func (t *Table) matchAndAggregate(querySpec *QuerySpec) {
	start := time.Now()

	querySpec.Table = t
	blockSpecs := searchBlocks(querySpec, t.BlockList)
	querySpec.ResetResults()

	// COMBINE THE PER BLOCK RESULTS
	resultSpec := combineResults(querySpec, blockSpecs)

	aend := time.Now()
	Debug("AGGREGATING TOOK", aend.Sub(start))

	querySpec.Results = resultSpec.Results
	querySpec.TimeResults = resultSpec.TimeResults

	// Aggregating Matched Records
	matched := combineMatches(blockSpecs)
	if HoldMatches {
		querySpec.Matched = matched
	}

	end := time.Now()

	sortResults(querySpec)

	Debug(string(len(matched)), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}
