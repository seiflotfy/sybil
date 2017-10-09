package pkg

import "fmt"
import "bytes"

import "os"
import "encoding/gob"
import "runtime/debug"
import "time"
import "regexp"

type valueMap map[int64][]uint32

var cardinalityThreshold = 4
var debugRecordConsistency = false

func deltaEncodeCol(col valueMap) {
	for _, records := range col {
		prev := uint32(0)
		for i, v := range records {
			records[int32(i)] = v - prev
			prev = v

		}
	}
}

func deltaEncode(sameMap map[int16]valueMap) {
	for _, col := range sameMap {
		if len(col) <= CHUNK_SIZE/cardinalityThreshold {
			deltaEncodeCol(col)
		}
	}
}

// this is used to record the buckets when building the column
// blobs
func recordValue(sameMap map[int16]valueMap, index int32, name int16, value int64) {
	s, ok := sameMap[name]
	if !ok {
		sameMap[name] = valueMap{}
		s = sameMap[name]
	}

	vi := value

	s[vi] = append(s[vi], uint32(index))
}

func (tb *TableBlock) GetColumnInfo(name_id int16) *TableColumn {
	col, ok := tb.columns[name_id]
	if !ok {
		col = tb.newTableColumn()
		tb.columns[name_id] = col
	}

	return col
}

func (tb *TableBlock) SaveIntsToColumns(dirname string, same_ints map[int16]valueMap) {
	// now make the dir and shoot each blob out into a separate file

	// SAVED TO A SINGLE BLOCK ON DISK, NOW TO SAVE IT OUT TO SEPARATE VALUES
	os.MkdirAll(dirname, 0777)
	for k, v := range same_ints {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "SOMETHING IS PROBABLY AWRY")
			continue
		}
		intCol := NewSavedIntColumn()

		intCol.Name = col_name
		intCol.DeltaEncodedIDs = Opts.deltaEncodeRecordIDs

		max_r := 0
		record_to_value := make(map[uint32]int64)
		for bucket, records := range v {
			si := SavedIntBucket{Value: bucket, Records: records}
			intCol.Bins = append(intCol.Bins, si)
			for _, r := range records {
				record_to_value[r] = bucket
				if int(r) >= max_r {
					max_r = int(r) + 1
				}
			}

			// bookkeeping for info.db
			tb.update_int_info(k, bucket)
			tb.table.update_int_info(k, bucket)
		}

		intCol.BucketEncoded = true
		// the column is high cardinality?
		if len(intCol.Bins) > CHUNK_SIZE/cardinalityThreshold {
			intCol.BucketEncoded = false
			intCol.Bins = nil
			intCol.Values = make([]int64, max_r)
			intCol.ValueEncoded = Opts.deltaEncodeIntValues

			for r, val := range record_to_value {
				intCol.Values[r] = val
			}

			prev := int64(0)
			for r, val := range intCol.Values {
				if Opts.deltaEncodeIntValues {
					intCol.Values[r] = val - prev
					prev = val
				} else {
					intCol.Values[r] = val
				}
			}
		}

		col_fname := fmt.Sprintf("%s/int_%s.db", dirname, tb.get_string_for_key(k))

		var network bytes.Buffer // Stand-in for the network.

		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(intCol)

		if err != nil {
			Error("encode:", err)
		}

		action := "SERIALIZED"
		if intCol.BucketEncoded {
			action = "BUCKETED  "
		}

		Debug(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.recordList), ")")

		w, _ := os.Create(col_fname)

		network.WriteTo(w)
	}

}

func (tb *TableBlock) SaveSetsToColumns(dirname string, same_sets map[int16]valueMap) {
	for k, v := range same_sets {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			// TODO: validate what this means. I think it means reading 'null' values off disk
			// when pulling off incomplete records
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
			continue
		}
		setCol := SavedSetColumn{}
		setCol.Name = col_name
		setCol.DeltaEncodedIDs = Opts.deltaEncodeRecordIDs
		temp_block := newTableBlock()

		tb_col := tb.GetColumnInfo(k)
		temp_col := temp_block.GetColumnInfo(k)
		record_to_value := make(map[uint32][]int32)
		max_r := 0
		for bucket, records := range v {
			// migrating string definitions from column definitions
			str_val := tb_col.get_string_for_val(int32(bucket))
			str_id := temp_col.get_val_id(str_val)
			si := SavedSetBucket{Value: int32(str_id), Records: records}
			setCol.Bins = append(setCol.Bins, si)
			for _, r := range records {
				_, ok := record_to_value[r]
				if int(r) >= max_r {
					max_r = int(r) + 1
				}

				if !ok {
					record_to_value[r] = make([]int32, 0)

				}

				record_to_value[r] = append(record_to_value[r], str_id)
			}
		}

		setCol.StringTable = make([]string, len(temp_col.StringTable))
		for str, id := range temp_col.StringTable {
			setCol.StringTable[id] = str
		}

		// the column is high cardinality?
		setCol.BucketEncoded = true
		if len(setCol.Bins) > CHUNK_SIZE/cardinalityThreshold {
			setCol.BucketEncoded = false
			setCol.Bins = nil
			setCol.Values = make([][]int32, max_r)
			for k, v := range record_to_value {
				setCol.Values[k] = v
			}
		}

		col_fname := fmt.Sprintf("%s/set_%s.db", dirname, tb.get_string_for_key(k))

		var network bytes.Buffer // Stand-in for the network.

		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(setCol)

		if err != nil {
			Error("encode:", err)
		}

		action := "SERIALIZED"
		if setCol.BucketEncoded {
			action = "BUCKETED  "
		}

		Debug(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.recordList), ")")

		w, _ := os.Create(col_fname)
		network.WriteTo(w)

	}
}

func (tb *TableBlock) SaveStrsToColumns(dirname string, same_strs map[int16]valueMap) {
	for k, v := range same_strs {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			// TODO: validate what this means. I think it means reading 'null' values off disk
			// when pulling off incomplete records
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
			continue
		}
		strCol := NewSavedStrColumn()
		strCol.Name = col_name
		strCol.DeltaEncodedIDs = Opts.deltaEncodeRecordIDs
		temp_block := newTableBlock()

		temp_col := temp_block.GetColumnInfo(k)
		tb_col := tb.GetColumnInfo(k)
		record_to_value := make(map[uint32]int32)
		max_r := 0
		for bucket, records := range v {

			// migrating string definitions from column definitions
			str_id := temp_col.get_val_id(tb_col.get_string_for_val(int32(bucket)))

			si := SavedStrBucket{Value: str_id, Records: records}
			strCol.Bins = append(strCol.Bins, si)
			for _, r := range records {
				record_to_value[r] = str_id
				if r >= uint32(max_r) {
					max_r = int(r) + 1
				}
			}

			// also bookkeeping to be used later inside the block info.db, IMO
			tb.update_str_info(k, int(bucket), len(records))
		}

		strCol.BucketEncoded = true
		// the column is high cardinality?
		if len(strCol.Bins) > CHUNK_SIZE/cardinalityThreshold {
			strCol.BucketEncoded = false
			strCol.Bins = nil
			strCol.Values = make([]int32, max_r)
			for k, v := range record_to_value {
				strCol.Values[k] = v
			}
		}

		for _, bucket := range strCol.Bins {
			first_val := bucket.Records[0]
			if first_val > 1000 && debugRecordConsistency {
				Warn(k, bucket.Value, "FIRST RECORD IS", first_val)
			}
		}

		tb.get_str_info(k).prune()

		strCol.StringTable = make([]string, len(temp_col.StringTable))
		for str, id := range temp_col.StringTable {
			strCol.StringTable[id] = str
		}

		col_fname := fmt.Sprintf("%s/str_%s.db", dirname, tb.get_string_for_key(k))

		var network bytes.Buffer // Stand-in for the network.

		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(strCol)

		if err != nil {
			Error("encode:", err)
		}

		action := "SERIALIZED"
		if strCol.BucketEncoded {
			action = "BUCKETED  "
		}

		Debug(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.recordList), ")")

		w, _ := os.Create(col_fname)
		network.WriteTo(w)

	}
}

type SavedIntInfo map[string]*IntInfo
type SavedStrInfo map[string]*StrInfo

func (tb *TableBlock) SaveInfoToColumns(dirname string) {
	records := tb.recordList

	// Now to save block info...
	col_fname := fmt.Sprintf("%s/info.db", dirname)

	var network bytes.Buffer // Stand-in for the network.

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)

	savedIntInfo := SavedIntInfo{}
	savedStrInfo := SavedStrInfo{}
	if tb.Info != nil {
		if tb.Info.IntInfoMap != nil {
			savedIntInfo = tb.Info.IntInfoMap
		}
		if tb.Info.StrInfoMap != nil {
			savedStrInfo = tb.Info.StrInfoMap
		}
	}

	for k, v := range tb.IntInfo {
		name := tb.get_string_for_key(k)
		savedIntInfo[name] = v
	}

	for k, v := range tb.StrInfo {
		name := tb.get_string_for_key(k)
		savedStrInfo[name] = v
	}

	colInfo := SavedColumnInfo{NumRecords: int32(len(records)), IntInfoMap: savedIntInfo, StrInfoMap: savedStrInfo}
	err := enc.Encode(colInfo)

	if err != nil {
		Error("encode:", err)
	}

	length := len(records)
	if length == 0 {
		length = 1
	}

	if debugTiming {
		Debug("SERIALIZED BLOCK INFO", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/length, ")")
	}

	w, _ := os.Create(col_fname)
	network.WriteTo(w)
}

type SeparatedColumns struct {
	ints map[int16]valueMap
	strs map[int16]valueMap
	sets map[int16]valueMap
}

func (tb *TableBlock) SeparateRecordsIntoColumns() SeparatedColumns {
	records := tb.recordList

	// making a cross section of records that share values
	// goes from fieldname{} -> value{} -> record
	same_ints := make(map[int16]valueMap)
	same_strs := make(map[int16]valueMap)
	same_sets := make(map[int16]valueMap)

	// parse record list and transfer book keeping data into the current
	// table block, as well as separate record values by column type
	for i, r := range records {
		for k, v := range r.Ints {
			if r.Populated[k] == INT_VAL {
				recordValue(same_ints, int32(i), int16(k), int64(v))
			}
		}
		for k, v := range r.Strs {
			// transition key from the
			col := r.block.GetColumnInfo(int16(k))
			new_col := tb.GetColumnInfo(int16(k))

			v_name := col.get_string_for_val(int32(v))
			v_id := new_col.get_val_id(v_name)

			// record the transitioned key
			if r.Populated[k] == STR_VAL {
				recordValue(same_strs, int32(i), int16(k), int64(v_id))
			}
		}
		for k, v := range r.SetMap {
			col := r.block.GetColumnInfo(int16(k))
			new_col := tb.GetColumnInfo(int16(k))
			if r.Populated[k] == SET_VAL {
				for _, iv := range v {
					v_name := col.get_string_for_val(int32(iv))
					v_id := new_col.get_val_id(v_name)
					recordValue(same_sets, int32(i), int16(k), int64(v_id))
				}
			}
		}
	}

	if Opts.deltaEncodeRecordIDs {
		deltaEncode(same_ints)
		deltaEncode(same_strs)
		deltaEncode(same_sets)
	}

	ret := SeparatedColumns{ints: same_ints, strs: same_strs, sets: same_sets}
	return ret

}

func (tb *TableBlock) SaveToColumns(filename string) bool {
	dirname := filename

	// Important to set the BLOCK's dirName so we can keep track
	// of the various block infos
	tb.Name = dirname

	defer tb.table.ReleaseBlockLock(filename)
	if tb.table.GrabBlockLock(filename) == false {
		Debug("Can't grab lock to save block", filename)
		return false
	}

	partialname := fmt.Sprintf("%s.partial", dirname)
	oldblock := fmt.Sprintf("%s.old", dirname)

	start := time.Now()
	old_percent := debug.SetGCPercent(-1)
	separated_columns := tb.SeparateRecordsIntoColumns()
	end := time.Now()
	Debug("COLLATING BLOCKS TOOK", end.Sub(start))

	tb.SaveIntsToColumns(partialname, separated_columns.ints)
	tb.SaveStrsToColumns(partialname, separated_columns.strs)
	tb.SaveSetsToColumns(partialname, separated_columns.sets)
	tb.SaveInfoToColumns(partialname)

	end = time.Now()
	Debug("FINISHED BLOCK", partialname, "RELINKING TO", dirname, "TOOK", end.Sub(start))

	debug.SetGCPercent(old_percent)

	// TODO: Add a stronger consistency check here
	// For now, we load info.db and check NumRecords inside it to prevent
	// catastrophics, but we could load everything potentially
	start = time.Now()
	nb := tb.table.LoadBlockFromDir(partialname, nil, false)
	end = time.Now()

	// TODO:
	if nb == nil || nb.Info.NumRecords != int32(len(tb.recordList)) {
		Error("COULDNT VALIDATE CONSISTENCY FOR RECENTLY SAVED BLOCK!", filename)
	}

	if debugRecordConsistency {
		nb = tb.table.LoadBlockFromDir(partialname, nil, true)
		if nb == nil || len(nb.recordList) != len(tb.recordList) {
			Error("DEEP VALIDATION OF BLOCK FAILED CONSISTENCY CHECK!", filename)
		}
	}

	Debug("VALIDATED NEW BLOCK HAS", nb.Info.NumRecords, "RECORDS, TOOK", end.Sub(start))

	os.RemoveAll(oldblock)
	err := renameAndMod(dirname, oldblock)
	if err != nil {
		Error("ERROR RENAMING BLOCK", dirname, oldblock, err)
	}
	err = renameAndMod(partialname, dirname)
	if err != nil {
		Error("ERROR RENAMING PARTIAL", partialname, dirname, err)
	}

	if err == nil {
		os.RemoveAll(oldblock)
	} else {
		Error("ERROR SAVING BLOCK", partialname, dirname, err)
	}

	Debug("RELEASING BLOCK", tb.Name)
	return true

}

func (tb *TableBlock) unpackStrCol(dec *fileDecoder, info SavedColumnInfo) {
	records := tb.recordList[:]

	into := &SavedStrColumn{}
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
		return
	}

	string_lookup := make(map[int32]string)
	key_table_len := len(tb.table.KeyTable)
	col_id := tb.table.get_key_id(into.Name)

	if int(col_id) >= key_table_len {
		Debug("IGNORING COLUMN", into.Name, "SINCE ITS NOT IN KEY TABLE IN BLOCK", tb.Name)
		return
	}

	col := tb.GetColumnInfo(col_id)
	// unpack the string table

	// Run our replacements!
	StrReplace, ok := Opts.StrReplaceMENTS[into.Name]
	bucket_replace := make(map[int32]int32)
	var re *regexp.Regexp
	if ok {
		re, err = regexp.Compile(StrReplace.pattern)
	}

	for k, v := range into.StringTable {
		var nv = v
		if re != nil {
			nv = re.ReplaceAllString(v, StrReplace.replace)
		}

		existing_key, exists := col.StringTable[nv]

		v = nv

		if exists {
			bucket_replace[int32(k)] = existing_key
		} else {
			bucket_replace[int32(k)] = int32(k)
			col.StringTable[v] = int32(k)
		}

		string_lookup[int32(k)] = v
	}

	col.val_string_id_lookup = string_lookup

	isPathCol := false
	if Flags.PathKey != nil {
		isPathCol = into.Name == *Flags.PathKey
	}
	var record *Record
	var r uint32

	if into.BucketEncoded {
		prev := uint32(0)
		did := into.DeltaEncodedIDs

		for _, bucket := range into.Bins {
			prev = 0
			value := bucket.Value
			new_value, should_replace := bucket_replace[value]
			if should_replace {
				value = new_value
			}

			cast_value := StrField(new_value)
			for _, r = range bucket.Records {

				if did {
					r = prev + r
				}

				prev = r
				record = records[r]

				if debugRecordConsistency {
					if record.Populated[col_id] != _NO_VAL {
						Error("OVERWRITING RECORD VALUE", record, into.Name, col_id, bucket.Value)
					}
				}

				records[r].Populated[col_id] = STR_VAL
				records[r].Strs[col_id] = cast_value

				if isPathCol {
					record.Path = string_lookup[new_value]
				}
			}
		}

	} else {
		for r, v := range into.Values {
			new_value, should_replace := bucket_replace[v]
			if should_replace {
				v = new_value
			}

			records[r].Strs[col_id] = StrField(v)
			records[r].Populated[col_id] = STR_VAL
		}

	}
}

func (tb *TableBlock) unpackSetCol(dec *fileDecoder, info SavedColumnInfo) {
	records := tb.recordList

	saved_col := NewSavedSetColumn()
	into := &saved_col
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
	}

	col_id := tb.table.get_key_id(into.Name)
	string_lookup := make(map[int32]string)

	col := tb.GetColumnInfo(col_id)
	// unpack the string table
	for k, v := range into.StringTable {
		col.StringTable[v] = int32(k)
		string_lookup[int32(k)] = v
	}
	col.val_string_id_lookup = string_lookup

	if into.BucketEncoded {
		for _, bucket := range into.Bins {
			// DONT FORGET TO DELTA UNENCODE THE RECORD VALUES
			prev := uint32(0)
			for _, r := range bucket.Records {
				if into.DeltaEncodedIDs {
					r = r + prev
				}

				cur_set, ok := records[r].SetMap[col_id]
				if !ok {
					cur_set = make(SetField, 0)
				}

				cur_set = append(cur_set, bucket.Value)
				records[r].SetMap[col_id] = cur_set

				records[r].Populated[col_id] = SET_VAL
				prev = r
			}

		}
	} else {
		for r, v := range into.Values {
			cur_set, ok := records[r].SetMap[col_id]
			if !ok {
				cur_set = make(SetField, 0)
				records[r].SetMap[col_id] = cur_set
			}

			records[r].SetMap[col_id] = SetField(v)
			records[r].Populated[col_id] = SET_VAL
		}
	}
}

func (tb *TableBlock) unpackIntCol(dec *fileDecoder, info SavedColumnInfo) {
	records := tb.recordList[:]

	into := &SavedIntColumn{}
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
	}

	col_id := tb.table.get_key_id(into.Name)

	is_TimeCol := false
	if Flags.TimeCol != nil {
		is_TimeCol = into.Name == *Flags.TimeCol
	}

	if into.BucketEncoded {
		for _, bucket := range into.Bins {
			if *Flags.UpdateTableInfo {
				tb.update_int_info(col_id, bucket.Value)
				tb.table.update_int_info(col_id, bucket.Value)
			}

			// DONT FORGET TO DELTA UNENCODE THE RECORD VALUES
			prev := uint32(0)
			for _, r := range bucket.Records {
				if into.DeltaEncodedIDs {
					r = r + prev
				}

				if debugRecordConsistency {
					if records[r].Populated[col_id] != _NO_VAL {
						Error("OVERWRITING RECORD VALUE", records[r], into.Name, col_id, bucket.Value)
					}
				}

				records[r].Ints[col_id] = IntField(bucket.Value)
				records[r].Populated[col_id] = INT_VAL
				prev = r

				if is_TimeCol {
					records[r].Timestamp = bucket.Value
				}

			}

		}
	} else {

		prev := int64(0)
		for r, v := range into.Values {
			if *Flags.UpdateTableInfo {
				tb.update_int_info(col_id, v)
				tb.table.update_int_info(col_id, v)
			}

			if into.ValueEncoded {
				v = v + prev
			}

			records[r].Ints[col_id] = IntField(v)
			records[r].Populated[col_id] = INT_VAL

			if is_TimeCol {
				records[r].Timestamp = v
			}

			if into.ValueEncoded {
				prev = v
			}

		}
	}
}
