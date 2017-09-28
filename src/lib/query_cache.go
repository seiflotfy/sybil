package sybil

import "encoding/gob"
import "crypto/md5"
import "bytes"
import "fmt"
import "path"
import "io/ioutil"
import "os"

type QueryCacheKey struct {
	Filters      []Filter
	Groups       []SavedGrouping
	Aggregations []SavedAggregation

	OrderBy    string
	Limit      int16
	TimeBucket int

	CachedResults SavedQueryResults
}

type SavedQueryResults struct {
	//	Cumulative   *Result
	Results     ResultMap
	TimeResults map[int]ResultMap
	//	Sorted       []*Result
	//	Matched      RecordList
	//	MatchedCount int
	//	Sessions     SessionList
}

type SavedGrouping struct {
	Name string
}

type SavedAggregation struct {
	Op   string
	Name string
	Type string
}

// for a per block query cache, we will have to clamp the time filters to the
// query block's extents.

func (querySpec *QuerySpec) GetRelevantFilters(blockname string) []Filter {

	filters := make([]Filter, 0)
	if querySpec == nil {
		return filters
	}

	t := querySpec.Table

	info := t.LoadBlockInfo(blockname)

	max_record := Record{Ints: IntArr{}, Strs: StrArr{}}
	min_record := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfoMap) == 0 {
		return filters
	}

	for field_name, _ := range info.StrInfoMap {
		field_id := t.get_key_id(field_name)
		min_record.ResizeFields(field_id)
		max_record.ResizeFields(field_id)
	}

	for field_name, field_info := range info.IntInfoMap {
		field_id := t.get_key_id(field_name)
		min_record.ResizeFields(field_id)
		max_record.ResizeFields(field_id)

		min_record.Ints[field_id] = IntField(field_info.Min)
		max_record.Ints[field_id] = IntField(field_info.Max)

		min_record.Populated[field_id] = INT_VAL
		max_record.Populated[field_id] = INT_VAL
	}

	for _, f := range querySpec.Filters {
		// make the minima record and the maxima records...
		switch f.(type) {
		case IntFilter:
			if f.Filter(&min_record) && f.Filter(&max_record) {
			} else {
				filters = append(filters, f)
			}

		default:
			filters = append(filters, f)
		}
	}

	return filters

}

func (qs *QuerySpec) GetCacheStruct(blockname string) QueryCacheKey {
	cache_spec := QueryCacheKey{}

	// CLAMP OUT FILTERS
	cache_spec.Filters = qs.GetRelevantFilters(blockname)
	cache_spec.Groups = make([]SavedGrouping, 0)
	for _, g := range qs.Groups {
		sg := SavedGrouping{g.name}
		cache_spec.Groups = append(cache_spec.Groups, sg)
	}

	cache_spec.Aggregations = make([]SavedAggregation, 0)
	for _, g := range qs.Aggregations {
		sg := SavedAggregation{g.op, g.name, g.hist_type}
		cache_spec.Aggregations = append(cache_spec.Aggregations, sg)
	}

	cache_spec.Limit = qs.Limit
	cache_spec.TimeBucket = qs.TimeBucket

	return cache_spec
}
func (qs *QuerySpec) GetCacheKey(blockname string) string {
	cache_spec := qs.GetCacheStruct(blockname)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cache_spec)
	if err != nil {
		Warn("encode:", err)
		return ""
	}

	h := md5.New()
	h.Write(buf.Bytes())

	ret := fmt.Sprintf("%x", h.Sum(nil))
	return ret
}

func (qs *QuerySpec) LoadCachedResults(blockname string) bool {
	if *FLAGS.SAMPLES {
		return false
	}

	cache_key := qs.GetCacheKey(blockname)

	cache_dir := path.Join(blockname, "cache")
	cache_name := fmt.Sprintf("%s.db", cache_key)
	filename := path.Join(cache_dir, cache_name)

	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return false
	}

	cachedSpec := SavedQueryResults{}
	dec := gob.NewDecoder(file)
	err = dec.Decode(&cachedSpec)
	if err != nil {
		Debug("ERROR DECODING CACHED FILE", err)
		return false
	}

	qs.Results = cachedSpec.Results
	qs.TimeResults = cachedSpec.TimeResults
	//	qs.Cumulative = cachedSpec.Cumulative
	//	qs.Sorted = cachedSpec.Sorted
	//	qs.Matched = cachedSpec.Matched

	return true
}

func (qs *QuerySpec) SaveCachedResults(blockname string) {
	if *FLAGS.CACHED_QUERIES == false {
		return
	}

	if *FLAGS.SAMPLES {
		return
	}

	info := qs.Table.LoadBlockInfo(blockname)

	if info.NumRecords < int32(CHUNK_SIZE) {
		return
	}

	cache_key := qs.GetCacheKey(blockname)

	cachedInfo := SavedQueryResults{}
	cachedInfo.Results = qs.Results
	cachedInfo.TimeResults = qs.TimeResults

	cache_dir := path.Join(blockname, "cache")
	os.MkdirAll(cache_dir, 0777)

	cache_name := fmt.Sprintf("%s.db", cache_key)
	filename := path.Join(cache_dir, cache_name)
	tempfile, err := ioutil.TempFile(cache_dir, cache_name)
	if err != nil {
		Debug("TEMPFILE ERROR", err)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(cachedInfo)

	if err != nil {
		Warn("cached query encoding error:", err)
		return
	}

	if err != nil {
		Warn("ERROR CREATING TEMP FILE FOR QUERY CACHED INFO", err)
		return
	}

	_, err = buf.WriteTo(tempfile)
	if err != nil {
		Warn("ERROR SAVING QUERY CACHED INFO INTO TEMPFILE", err)
		return
	}

	RenameAndMod(tempfile.Name(), filename)

	return

}
