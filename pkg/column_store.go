package pkg

// the blockVersion is how we get hints about decoding blocks for backwards
// compatibility. at least, it will be in the future
var blockVersion = int32(1)

// Before we save the new record list in a table, we tend to sort by time
type recordList []*Record
type sortRecordsByTime struct {
	recordList
}

func (a recordList) Len() int      { return len(a) }
func (a recordList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortRecordsByTime) Less(i, j int) bool {
	t1 := a.recordList[i].Timestamp
	t2 := a.recordList[j].Timestamp

	return t1 < t2
}

type SavedIntBucket struct {
	Value   int64
	Records []uint32
}

type SavedSetBucket struct {
	Value   int32
	Records []uint32
}

type SavedStrBucket struct {
	Value   int32
	Records []uint32
}

type SavedBlockCache map[string]*SavedColumnInfo

type SavedColumnInfo struct {
	NumRecords int32

	StrInfoMap SavedStrInfo
	IntInfoMap SavedIntInfo
}

type SavedIntColumn struct {
	Name            string
	DeltaEncodedIDs bool
	ValueEncoded    bool
	BucketEncoded   bool
	Bins            []SavedIntBucket
	Values          []int64
	VERSION         int32
}

type SavedStrColumn struct {
	Name            string
	DeltaEncodedIDs bool
	BucketEncoded   bool
	Bins            []SavedStrBucket
	Values          []int32
	StringTable     []string
	VERSION         int32
}

type SavedSetColumn struct {
	Name            string
	Bins            []SavedSetBucket
	Values          [][]int32
	StringTable     []string
	DeltaEncodedIDs bool
	BucketEncoded   bool
	VERSION         int32
}

func NewSavedIntColumn() SavedIntColumn {
	ret := SavedIntColumn{}

	ret.VERSION = blockVersion
	return ret

}
func NewSavedStrColumn() SavedStrColumn {
	ret := SavedStrColumn{}

	ret.VERSION = blockVersion
	return ret

}

func NewSavedSetColumn() SavedSetColumn {
	ret := SavedSetColumn{}

	ret.VERSION = blockVersion

	return ret
}
