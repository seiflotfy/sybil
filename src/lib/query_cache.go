package sybil

import "encoding/gob"
import "crypto/md5"
import "bytes"
import "fmt"
import "path"
import "io/ioutil"
import "os"
import "compress/gzip"

type QueryCacheKey struct {
	Filters      []Filter
	Groups       []Grouping
	Aggregations []Aggregation

	OrderBy    string
	Limit      int16
	TimeBucket int

	CachedResults SavedQueryResults
}

func (t *Table) getCachedQueryForBlock(dirname string, querySpec *QuerySpec) (*TableBlock, *QuerySpec) {

	if *FLAGS.CACHED_QUERIES == false {
		return nil, nil
	}

	tb := newTableBlock()
	tb.Name = dirname
	tb.table = t
	info := t.LoadBlockInfo(dirname)

	if info == nil {
		Debug("NO INFO FOR", dirname)
		return nil, nil
	}

	if info.NumRecords <= 0 {
		Debug("NO RECORDS FOR", dirname)
		return nil, nil
	}

	tb.Info = info

	blockQuery := CopyQuerySpec(querySpec)
	if blockQuery.LoadCachedResults(tb.Name) {
		t.block_m.Lock()
		t.BlockList[dirname] = &tb
		t.block_m.Unlock()

		return &tb, blockQuery

	}

	return nil, nil

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

	// CLAMP OUT TRIVIAL FILTERS
	cache_spec.Filters = qs.GetRelevantFilters(blockname)

	cache_spec.Groups = qs.Groups
	cache_spec.Aggregations = qs.Aggregations

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
	if *FLAGS.CACHED_QUERIES == false {
		return false
	}

	if *FLAGS.SAMPLES {
		return false

	}

	cache_key := qs.GetCacheKey(blockname)

	cache_dir := path.Join(blockname, "cache")
	cache_name := fmt.Sprintf("%s.db", cache_key)
	filename := path.Join(cache_dir, cache_name)

	dec := GetFileDecoder(filename)
	cachedSpec := SavedQueryResults{}
	err := dec.Decode(&cachedSpec)

	if err != nil {
		return false
	}

	qs.SavedQueryResults = cachedSpec

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

	cachedInfo := qs.SavedQueryResults

	cache_dir := path.Join(blockname, "cache")
	os.MkdirAll(cache_dir, 0777)

	cache_name := fmt.Sprintf("%s.db.gz", cache_key)
	filename := path.Join(cache_dir, cache_name)
	tempfile, err := ioutil.TempFile(cache_dir, cache_name)
	if err != nil {
		Debug("TEMPFILE ERROR", err)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(cachedInfo)

	var gbuf bytes.Buffer
	w := gzip.NewWriter(&gbuf)
	w.Write(buf.Bytes())
	w.Close() // You must close this first to flush the bytes to the buffer.

	if err != nil {
		Warn("cached query encoding error:", err)
		return
	}

	if err != nil {
		Warn("ERROR CREATING TEMP FILE FOR QUERY CACHED INFO", err)
		return
	}

	_, err = gbuf.WriteTo(tempfile)
	if err != nil {
		Warn("ERROR SAVING QUERY CACHED INFO INTO TEMPFILE", err)
		return
	}

	tempfile.Close()
	err = RenameAndMod(tempfile.Name(), filename)
	if err != nil {
		Warn("ERROR RENAMING", tempfile.Name())
	}

	return

}
