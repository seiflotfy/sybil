package pkg

import "strconv"
import "strings"
import "math"
import "math/rand"
import "testing"

func TestTableLoadRowRecords(test *testing.T) {
	delete_test_db()

	block_count := 3
	add_records(func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	t := GetTable(TEST_TABLE_NAME)
	t.ingestRecords("ingest")

	unload_test_table()
	nt := GetTable(TEST_TABLE_NAME)

	nt.LoadRecords(nil)

	if len(nt.RowBlock.recordList) != CHUNK_SIZE*block_count {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.recordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	querySpec := new_query_spec()

	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.matchAndAggregate(querySpec)

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, groupDelimiter, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
		}
	}

}
