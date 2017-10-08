package pkg

import "flag"

var (
	falseFlag = false
	trueFlag  = true
	testMode  = false
	enableLua = false
)

type FlagDefs struct {
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

	Time        *bool
	TimeCol     *string
	Time_BUCKET *int
	HIST_BUCKET *int
	HDR_HIST    *bool
	LOG_HIST    *bool

	FIELD_SEPARATOR    *string
	FILTER_SEPARATOR   *string
	PRINT_KEYS         *bool
	LOAD_AND_QUERY     *bool
	LOAD_THEN_QUERY    *bool
	READ_INGESTION_LOG *bool
	READ_ROWSTORE      *bool
	SKIP_COMPACT       *bool

	PROFILE     *bool
	PROFILE_MEM *bool

	RECYCLE_MEM    *bool
	CACHED_QUERIES *bool

	WEIGHT_COL *string

	LIMIT *int

	DEBUG *bool
	JSON  *bool
	GC    *bool

	DIR        *string
	SORT       *string
	TABLE      *string
	PRINT_INFO *bool
	SAMPLES    *bool

	LUA     *bool
	LUAFILE *string

	UPDATE_TABLE_INFO *bool
	SKIP_OUTLIERS     *bool

	// Join keys
	JOIN_TABLE *string
	JOIN_KEY   *string
	JOIN_GROUP *string

	// Sessionization stuff
	SESSION_CUTOFF *int
	RETENTION      *bool
	PATH_KEY       *string
	PATH_LENGTH    *int

	// STATS
	ANOVA_ICC *bool
}

type StrReplace struct {
	pattern string
	replace string
}

type OptionDefs struct {
	SORT_COUNT              string
	SAMPLES                 bool
	StrReplaceMENTS         map[string]StrReplace
	WEIGHT_COL              bool
	WEIGHT_COL_ID           int16
	DELTA_ENCODE_INT_VALUES bool
	DELTA_ENCODE_RECORD_IDS bool
	WRITE_BLOCK_INFO        bool
	TimeSERIES              bool
	TimeColID               int16
	TimeFormat              string
	GROUP_BY                []string
}

// TODO: merge these two into one thing
// current problem is that FLAGS needs pointers
var FLAGS = FlagDefs{}
var OPTS = OptionDefs{}
var EMPTY = ""

func setDefaults() {
	OPTS.SORT_COUNT = "$COUNT"
	OPTS.SAMPLES = false
	OPTS.WEIGHT_COL = false
	OPTS.WEIGHT_COL_ID = int16(0)
	OPTS.DELTA_ENCODE_INT_VALUES = true
	OPTS.DELTA_ENCODE_RECORD_IDS = true
	OPTS.WRITE_BLOCK_INFO = false
	OPTS.TimeSERIES = false
	OPTS.TimeFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

	FLAGS.GC = &trueFlag
	FLAGS.JSON = &falseFlag
	FLAGS.PRINT = &trueFlag
	FLAGS.EXPORT = &falseFlag

	FLAGS.SKIP_COMPACT = &falseFlag

	FLAGS.PRINT_KEYS = &OPTS.TimeSERIES
	FLAGS.LOAD_AND_QUERY = &trueFlag
	FLAGS.LOAD_THEN_QUERY = &falseFlag
	FLAGS.READ_INGESTION_LOG = &falseFlag
	FLAGS.READ_ROWSTORE = &falseFlag
	FLAGS.ANOVA_ICC = &falseFlag
	FLAGS.DIR = flag.String("dir", "./db/", "Directory to store DB files")
	FLAGS.TABLE = flag.String("table", "", "Table to operate on [REQUIRED]")

	FLAGS.DEBUG = flag.Bool("debug", false, "enable debug logging")
	FLAGS.FIELD_SEPARATOR = flag.String("field-separator", ",", "Field separator used in command line params")
	FLAGS.FILTER_SEPARATOR = flag.String("filter-separator", ":", "Filter separator used in filters")

	FLAGS.UPDATE_TABLE_INFO = &falseFlag
	FLAGS.SKIP_OUTLIERS = &trueFlag
	FLAGS.SAMPLES = &falseFlag
	FLAGS.LUA = &falseFlag
	FLAGS.LUAFILE = &EMPTY

	FLAGS.RECYCLE_MEM = &trueFlag
	FLAGS.CACHED_QUERIES = &falseFlag

	FLAGS.HDR_HIST = &falseFlag
	FLAGS.LOG_HIST = &falseFlag

	DEFAULT_LIMIT := 100
	FLAGS.LIMIT = &DEFAULT_LIMIT

	FLAGS.PROFILE = &falseFlag
	FLAGS.PROFILE_MEM = &falseFlag
	if PROFILER_ENABLED {
		FLAGS.PROFILE = flag.Bool("profile", false, "turn profiling on?")
		FLAGS.PROFILE_MEM = flag.Bool("mem", false, "turn memory profiling on")
	}

	initLua()

}
