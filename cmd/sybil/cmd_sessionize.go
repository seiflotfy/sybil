package main

import sybil "github.com/logv/sybil/pkg"

import "flag"
import "time"
import "runtime/debug"
import "strings"

func addSessionFlags() {
	sybil.Flags.PRINT = flag.Bool("print", false, "Print some records")
	sybil.Flags.TimeCol = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.Flags.SessionCol = flag.String("session", "", "Column to use for sessionizing")
	sybil.Flags.SessionCutOff = flag.Int("cutoff", 60, "distance between consecutive events before generating a new session")
	sybil.Flags.JoinTable = flag.String("join-table", "", "dataset to join against for session summaries")
	sybil.Flags.JoinKey = flag.String("join-key", "", "Field to join sessionid against in join-table")
	sybil.Flags.JoinGroup = flag.String("join-group", "", "Group by columns to pull from join record")
	sybil.Flags.PathKey = flag.String("path-key", "", "Field to use for pathing")
	sybil.Flags.PathLength = flag.Int("path-length", 3, "Size of paths to histogram")
	sybil.Flags.Retention = flag.Bool("calendar", false, "calculate retention calendars")
	sybil.Flags.JSON = flag.Bool("json", false, "print results in JSON form")

	sybil.Flags.IntFilters = flag.String("int-filter", "", "Int filters, format: col:op:val")
	sybil.Flags.StrFilters = flag.String("str-filter", "", "Str filters, format: col:op:val")
	sybil.Flags.SetFilters = flag.String("set-filter", "", "Set filters, format: col:op:val")

	sybil.Flags.StrReplace = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	sybil.Flags.Limit = flag.Int("limit", 100, "Number of results to return")
}

func runSessionizeCmdLine() {
	addSessionFlags()
	flag.Parse()
	start := time.Now()

	table := *sybil.Flags.Table
	if table == "" {
		flag.PrintDefaults()
		return
	}

	tableNames := strings.Split(table, *sybil.Flags.FieldSeparator)
	sybil.Debug("LOADING TABLES", tableNames)

	tables := make([]*sybil.Table, 0)

	for _, tablename := range tableNames {
		t := sybil.GetTable(tablename)
		// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
		// THE RIGHT COLUMN ID
		t.LoadTableInfo()
		t.LoadRecords(nil)

		count := 0
		for _, block := range t.BlockList {
			count += int(block.Info.NumRecords)
		}

		sybil.Debug("WILL INSPECT", count, "RECORDS FROM", tablename)

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

		tables = append(tables, t)

	}

	debug.SetGCPercent(-1)
	if *sybil.Flags.Profile && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	filters := []sybil.Filter{}
	groupings := []sybil.Grouping{}
	aggs := []sybil.Aggregation{}
	query_params := sybil.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := sybil.QuerySpec{QueryParams: query_params}

	querySpec.Limit = int16(*sybil.Flags.Limit)

	if *sybil.Flags.SessionCol != "" {
		sessionSpec := sybil.NewSessionSpec()
		sybil.LoadAndSessionize(tables, &querySpec, &sessionSpec)
	}

	end := time.Now()
	sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
}
