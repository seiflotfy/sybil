package sybil_test

import sybil "./"

import "testing"
import "math/rand"
import "strconv"

func TestCachedQueries(test *testing.T) {
	delete_test_db()

	block_count := 5

	sybil.DELETE_BLOCKS_AFTER_QUERY = false
	sybil.FLAGS.CACHED_QUERIES = &sybil.TRUE

	var this_add_records = func(block_count int) {
		add_records(func(r *sybil.Record, i int) {
			age := int64(rand.Intn(20)) + 10

			age_str := strconv.FormatInt(int64(age), 10)
			r.AddIntField("id", int64(i))
			r.AddIntField("age", age)
			r.AddStrField("age_str", age_str)
			r.AddSetField("age_set", []string{age_str})

		}, block_count)
		save_and_reload_table(test, block_count)

	}

	this_add_records(block_count)
	testCachedQueryFiles(test)
	delete_test_db()

	this_add_records(block_count)
	testCachedQueryConsistency(test)
	delete_test_db()

	this_add_records(block_count)
	testCachedBasicHist(test)
	delete_test_db()

	sybil.FLAGS.CACHED_QUERIES = &sybil.FALSE

}

func testCachedQueryFiles(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs, Table: nt}
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadAndQueryRecords(&loadSpec, nil)
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			test.Error("Test DB started with saved query results")
		}
	}

	nt.LoadAndQueryRecords(&loadSpec, &querySpec)

	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		// Test Filtering to 20..
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}
}

func testCachedQueryConsistency(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs, Table: nt}
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadAndQueryRecords(&loadSpec, &querySpec)
	copySpec := sybil.CopyQuerySpec(&querySpec)

	nt = sybil.GetTable(TEST_TABLE_NAME)

	// clear the copied query spec result map and look
	// at the cached query results

	copySpec.Results = make(sybil.ResultMap, 0)
	nt.LoadAndQueryRecords(&loadSpec, copySpec)

	if len(querySpec.Results) == 0 {
		test.Error("No Results for Query")
	}

	for k, v := range querySpec.Results {
		v2, ok := copySpec.Results[k]
		if !ok {
			test.Error("Result Mismatch!", k, v)
		}

		if v.Count != v2.Count {
			test.Error("Count Mismatch", v, v2, v.Count, v2.Count)
		}

		if v.Samples != v2.Samples {
			Debug(v, v2)
			test.Error("Samples Mismatch", v, v2, v.Samples, v2.Samples)
		}

	}

	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		// Test Filtering to 20..
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}

}

func testCachedBasicHist(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs, Table: nt}
	HIST := "hist"
	sybil.FLAGS.OP = &HIST
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadAndQueryRecords(&loadSpec, &querySpec)
	copySpec := sybil.CopyQuerySpec(&querySpec)

	nt = sybil.GetTable(TEST_TABLE_NAME)

	// clear the copied query spec result map and look
	// at the cached query results

	copySpec.Results = make(sybil.ResultMap, 0)
	nt.LoadAndQueryRecords(&loadSpec, copySpec)

	if len(querySpec.Results) == 0 {
		test.Error("No Results for Query")
	}

	for k, v := range querySpec.Results {
		v2, ok := copySpec.Results[k]
		if !ok {
			test.Error("Result Mismatch!", k, v)
		}

		if v.Count != v2.Count {
			test.Error("Count Mismatch", v, v2, v.Count, v2.Count)
		}

		if v.Samples != v2.Samples {
			Debug(v, v2)
			test.Error("Samples Mismatch", v, v2, v.Samples, v2.Samples)
		}

		for k, h := range v.Hists {
			h2, ok := v2.Hists[k]
			if !ok {
				test.Error("Missing Histogram", v, v2)
			}

			if h.StdDev() <= 0 {
				test.Error("Missing StdDev", h, h.StdDev())
			}

			if h.StdDev() != h2.StdDev() {
				test.Error("StdDev MisMatch", h, h2)
			}

		}

	}

	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		// Test Filtering to 20..
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}

}
