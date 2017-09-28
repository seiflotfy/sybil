package sybil_test

import sybil "./"

import "testing"
import "math/rand"
import "strconv"

func TestCachedQueries(test *testing.T) {
	delete_test_db()

	block_count := 5
	add_records(func(r *sybil.Record, i int) {
		age := int64(rand.Intn(20)) + 10

		age_str := strconv.FormatInt(int64(age), 10)
		r.AddIntField("id", int64(i))
		r.AddIntField("age", age)
		r.AddStrField("age_str", age_str)
		r.AddSetField("age_set", []string{age_str})

	}, block_count)

	save_and_reload_table(test, block_count)

	sybil.DELETE_BLOCKS_AFTER_QUERY = false
	sybil.FLAGS.CACHED_QUERIES = &sybil.TRUE

	testCachedTableQuery(test)

	delete_test_db()

	sybil.FLAGS.CACHED_QUERIES = &sybil.FALSE

}

func testCachedTableQuery(test *testing.T) {
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
			test.Error("Did not correctly save query results")
		}
	}
}
