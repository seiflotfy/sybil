package main

import sybil "github.com/logv/sybil/pkg"

import "fmt"
import "flag"
import "strings"
import "time"
import "path"
import "runtime/debug"

var maxRecordsNoGc = 4 * 1000 * 1000 // 4 million

var listTables *bool
var TimeFormat *string
var NO_RECYCLE_MEM *bool

func addQueryFlags() {

	sybil.Flags.PrintInfo = flag.Bool("info", false, "Print table info")
	sybil.Flags.Sort = flag.String("sort", sybil.Opts.SortCount, "Int Column to sort by")
	sybil.Flags.Limit = flag.Int("limit", 100, "Number of results to return")

	sybil.Flags.Time = flag.Bool("time", false, "make a time rollup")
	sybil.Flags.TimeCol = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.Flags.TimeBucket = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	sybil.Flags.WeightCol = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	sybil.Flags.Op = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	sybil.Flags.LogHist = flag.Bool("loghist", false, "Use nested logarithmic histograms")
	if sybil.ENABLE_HDR {
		sybil.Flags.HdrHist = flag.Bool("hdr", false, "Use HDR Histograms (can be slow)")
	}

	sybil.Flags.PRINT = flag.Bool("print", true, "Print some records")
	sybil.Flags.Samples = flag.Bool("samples", false, "Grab samples")
	sybil.Flags.IntFilters = flag.String("int-filter", "", "Int filters, format: col:op:val")

	sybil.Flags.HistBucket = flag.Int("int-bucket", 0, "Int hist bucket size")

	sybil.Flags.StrReplace = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	sybil.Flags.StrFilters = flag.String("str-filter", "", "Str filters, format: col:op:val")
	sybil.Flags.SetFilters = flag.String("set-filter", "", "Set filters, format: col:op:val")
	sybil.Flags.UpdateTableInfo = flag.Bool("update-info", false, "Re-compute cached column data")

	sybil.Flags.Ints = flag.String("int", "", "Integer values to aggregate")
	sybil.Flags.Strs = flag.String("str", "", "String values to load")
	sybil.Flags.Groups = flag.String("group", "", "values group by")

	sybil.Flags.EXPORT = flag.Bool("export", false, "export data to TSV")

	sybil.Flags.ReadRowStore = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	sybil.Flags.JSON = flag.Bool("json", false, "Print results in JSON format")
	sybil.Flags.ANOVA = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	if sybil.EnableLua {
		sybil.Flags.LUAFILE = flag.String("lua", "", "Script to execute with lua map reduce")
	}

	listTables = flag.Bool("tables", false, "List tables")

	TimeFormat = flag.String("time-format", "", "time format to use")
	NO_RECYCLE_MEM = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

	sybil.Flags.CachedQueries = flag.Bool("cache-queries", false, "Cache query results per block")

}

func runQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *listTables {
		sybil.PrintTables()
		return
	}

	if *TimeFormat != "" {
		sybil.Opts.TimeFormat = sybil.GetTimeFormat(*TimeFormat)
	}

	table := *sybil.Flags.Table
	if table == "" {
		flag.PrintDefaults()
		return
	}

	t := sybil.GetTable(table)
	if t.IsNotExist() {
		sybil.Error(t.Name, "table can not be loaded or does not exist in", *sybil.Flags.Dir)
	}

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)

	if *sybil.Flags.Groups != "" {
		groups = strings.Split(*sybil.Flags.Groups, *sybil.Flags.FieldSeparator)
		sybil.Opts.GroupBy = groups

	}

	if *sybil.Flags.LUAFILE != "" {
		sybil.SetLuaScript(*sybil.Flags.LUAFILE)
	}

	if *NO_RECYCLE_MEM == true {
		sybil.Flags.RecycleMem = &sybil.FalseFlag
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *sybil.Flags.Strs != "" {
		strs = strings.Split(*sybil.Flags.Strs, *sybil.Flags.FieldSeparator)
	}
	if *sybil.Flags.Ints != "" {
		ints = strings.Split(*sybil.Flags.Ints, *sybil.Flags.FieldSeparator)
	}
	if *sybil.Flags.Profile && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *sybil.Flags.LoadThenQuery {
		sybil.Flags.LoadAndQuery = &FalseFlag
	}

	if *sybil.Flags.ReadRowStore {
		sybil.Flags.ReadIngestionLog = &trueFlag
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	t.LoadTableInfo()
	t.LoadRecords(nil)

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	sybil.Debug("WILL INSPECT", count, "RECORDS")

	groupings := []sybil.Grouping{}
	for _, g := range groups {
		groupings = append(groupings, t.Grouping(g))
	}

	aggs := []sybil.Aggregation{}
	for _, agg := range ints {
		aggs = append(aggs, t.Aggregation(agg, *sybil.Flags.Op))
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	sybil.Debug("KEY TABLE", t.KeyTable)
	sybil.Debug("KEY TYPES", t.KeyTypes)

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			sybil.Error("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
			return
		}
	}

	loadSpec := t.NewLoadSpec()
	filterSpec := sybil.FilterSpec{Int: *sybil.Flags.IntFilters, Str: *sybil.Flags.StrFilters, Set: *sybil.Flags.SetFilters}
	filters := sybil.BuildFilters(t, &loadSpec, filterSpec)

	query_params := sybil.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := sybil.QuerySpec{QueryParams: query_params}

	for _, v := range groups {
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			loadSpec.Str(v)
		case sybil.INT_VAL:
			loadSpec.Int(v)
		default:
			t.PrintColInfo()
			fmt.Println("")
			sybil.Error("Unknown column type for column: ", v, t.GetColumnType(v))
		}

	}
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}

	if *sybil.Flags.Sort != "" {
		if *sybil.Flags.Sort != sybil.Opts.SortCount {
			loadSpec.Int(*sybil.Flags.Sort)
		}
		querySpec.OrderBy = *sybil.Flags.Sort
	} else {
		querySpec.OrderBy = ""
	}

	if *sybil.Flags.Time {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *sybil.Flags.TimeBucket
		sybil.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*sybil.Flags.TimeCol)
		TimeColID, ok := t.KeyTable[*sybil.Flags.TimeCol]
		if ok {
			sybil.Opts.TimeColID = TimeColID
		}
	}

	if *sybil.Flags.WeightCol != "" {
		sybil.Opts.WeightCol = true
		loadSpec.Int(*sybil.Flags.WeightCol)
		sybil.Opts.WeightColID = t.KeyTable[*sybil.Flags.WeightCol]
	}

	querySpec.Limit = int16(*sybil.Flags.Limit)

	if *sybil.Flags.Samples {
		sybil.HoldMatches = true
		sybil.DeleteBlocksAfterQuery = false

		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		t.LoadAndQueryRecords(&loadSpec, &querySpec)

		t.PrintSamples()

		return
	}

	if *sybil.Flags.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !*sybil.Flags.PrintInfo {
		// DISABLE GC FOR QUERY PATH
		sybil.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		sybil.Debug("USING LOAD SPEC", loadSpec)

		sybil.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *sybil.Flags.LoadAndQuery {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()

			if sybil.Flags.ANOVA != nil && *sybil.Flags.ANOVA {
				querySpec.CalculateICC()
			}
		}

	}

	if *sybil.Flags.EXPORT {
		sybil.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *sybil.Flags.PrintInfo {
		t := sybil.GetTable(table)
		sybil.Flags.LoadAndQuery = &FalseFlag

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
