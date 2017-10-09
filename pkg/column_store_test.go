package pkg

import "strconv"
import "math/rand"
import "testing"

func TestTableDigestRowRecords(test *testing.T) {
	delete_test_db()

	blockCount := 3
	add_records(func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	t := GetTable(TEST_TABLE_NAME)
	t.IngestRecords("ingest")

	unload_test_table()
	nt := GetTable(TEST_TABLE_NAME)
	DeleteBlocksAfterQuery = false
	Flags.ReadIngestionLog = &trueFlag

	nt.LoadTableInfo()
	nt.LoadRecords(nil)

	if len(nt.RowBlock.recordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.recordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	nt.DigestRecords()

	unload_test_table()

	readRowsOnly = false
	nt = GetTable(TEST_TABLE_NAME)
	nt.LoadRecords(nil)

	count := int32(0)
	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*CHUNK_SIZE) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)

	}

}
