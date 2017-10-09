package pkg

import "flag"

var (
	// FalseFlag ...
	FalseFlag = false
	trueFlag  = true
	testMode  = false
	// EnableLua ...
	EnableLua = false
	empty     = ""

	// TODO: merge these two into one thing
	// current problem is that Flags needs pointers

	// Flags ...
	Flags = flagDefs{}
	// Opts ...
	Opts = optionDefs{}
)

type flagDefs struct {
	Op         *string
	PRINT      *bool
	EXPORT     *bool
	IntFilters *string
	StrFilters *string
	StrReplace *string // regex replacement for strings
	SetFilters *string

	SessionCol *string
	Ints       *string
	Strs       *string
	Groups     *string

	AddRecords *int

	Time       *bool
	TimeCol    *string
	TimeBucket *int
	HistBucket *int
	HdrHist    *bool
	LogHist    *bool

	FieldSeparator   *string
	FilterSeparator  *string
	PrintKeys        *bool
	LoadAndQuery     *bool
	LoadThenQuery    *bool
	ReadIngestionLog *bool
	ReadRowStore     *bool
	SkipCompact      *bool

	Profile    *bool
	ProfileMem *bool

	RecycleMem    *bool
	CachedQueries *bool

	WeightCol *string

	Limit *int

	Debug *bool
	JSON  *bool
	GC    *bool

	Dir       *string
	Sort      *string
	Table     *string
	PrintInfo *bool
	Samples   *bool

	LUA     *bool
	LUAFILE *string

	UpdateTableInfo *bool
	skipOutliers    *bool

	// Join keys
	JoinTable *string
	JoinKey   *string
	JoinGroup *string

	// Sessionization stuff
	SessionCutOff *int
	Retention     *bool
	PathKey       *string
	PathLength    *int

	// STATS
	ANOVA *bool
}

// StrReplace ...
type StrReplace struct {
	pattern string
	replace string
}

type optionDefs struct {
	SortCount            string
	SAMPLES              bool
	StrReplaceMENTS      map[string]StrReplace
	WeightCol            bool
	WeightColID          int16
	deltaEncodeIntValues bool
	deltaEncodeRecordIDs bool
	WriteBlockInfo       bool
	TimeSeries           bool
	TimeColID            int16
	TimeFormat           string
	GroupBy              []string
}

func setDefaults() {
	Opts.SortCount = "$COUNT"
	Opts.SAMPLES = false
	Opts.WeightCol = false
	Opts.WeightColID = int16(0)
	Opts.deltaEncodeIntValues = true
	Opts.deltaEncodeRecordIDs = true
	Opts.WriteBlockInfo = false
	Opts.TimeSeries = false
	Opts.TimeFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

	Flags.GC = &trueFlag
	Flags.JSON = &FalseFlag
	Flags.PRINT = &trueFlag
	Flags.EXPORT = &FalseFlag

	Flags.SkipCompact = &FalseFlag

	Flags.PrintKeys = &Opts.TimeSeries
	Flags.LoadAndQuery = &trueFlag
	Flags.LoadThenQuery = &FalseFlag
	Flags.ReadIngestionLog = &FalseFlag
	Flags.ReadRowStore = &FalseFlag
	Flags.ANOVA = &FalseFlag
	Flags.Dir = flag.String("dir", "./db/", "Directory to store DB files")
	Flags.Table = flag.String("table", "", "Table to operate on [REQUIRED]")

	Flags.Debug = flag.Bool("debug", false, "enable debug logging")
	Flags.FieldSeparator = flag.String("field-separator", ",", "Field separator used in command line params")
	Flags.FilterSeparator = flag.String("filter-separator", ":", "Filter separator used in filters")

	Flags.UpdateTableInfo = &FalseFlag
	Flags.skipOutliers = &trueFlag
	Flags.Samples = &FalseFlag
	Flags.LUA = &FalseFlag
	Flags.LUAFILE = &empty

	Flags.RecycleMem = &trueFlag
	Flags.CachedQueries = &FalseFlag

	Flags.HdrHist = &FalseFlag
	Flags.LogHist = &FalseFlag

	defaultLimit := 100
	Flags.Limit = &defaultLimit

	Flags.Profile = &FalseFlag
	Flags.ProfileMem = &FalseFlag
	if PROFILER_ENABLED {
		Flags.Profile = flag.Bool("profile", false, "turn profiling on?")
		Flags.ProfileMem = flag.Bool("mem", false, "turn memory profiling on")
	}

	initLua()

}
