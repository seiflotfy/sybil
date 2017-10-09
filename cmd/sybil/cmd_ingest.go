package main

import sybil "github.com/logv/sybil/pkg"

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Dictionary map[string]interface{}

var JSON_PATH string

// how many times we try to grab table info when ingesting
var TABLE_INFO_GRABS = 10

func ingestDirecotry(r *sybil.Record, recordmap *Dictionary, prefix string) {
	for k, v := range *recordmap {
		key_name := fmt.Sprint(prefix, k)
		_, ok := EXCLUDES[key_name]
		if ok {
			continue
		}

		prefix_name := fmt.Sprint(key_name, "_")
		switch iv := v.(type) {
		case string:
			if INT_CAST[key_name] == true {
				val, err := strconv.ParseInt(iv, 10, 64)
				if err == nil {
					r.AddIntField(key_name, int64(val))
				}
			} else {
				r.AddStrField(key_name, iv)

			}
		case int64:
			r.AddIntField(key_name, int64(iv))
		case float64:
			r.AddIntField(key_name, int64(iv))
		// nested fields
		case map[string]interface{}:
			d := Dictionary(iv)
			ingestDirecotry(r, &d, prefix_name)
		// This is a set field
		case []interface{}:
			key_strs := make([]string, 0)
			for _, v := range iv {
				switch av := v.(type) {
				case string:
					key_strs = append(key_strs, av)
				case float64:
					key_strs = append(key_strs, fmt.Sprintf("%.0f", av))
				case int64:
					key_strs = append(key_strs, strconv.FormatInt(av, 64))
				}
			}

			r.AddSetField(key_name, key_strs)
		case nil:
		default:
			sybil.Debug(fmt.Sprintf("TYPE %T IS UNKNOWN FOR FIELD", iv), key_name)
		}
	}
}

var IMPORTED_COUNT = 0

func import_csv_records() {
	// For importing CSV records, we need to validate the headers, then we just
	// read in and fill out record fields!
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	header := scanner.Text()
	header_fields := strings.Split(header, ",")
	sybil.Debug("HEADER FIELDS FOR CSV ARE", header_fields)

	t := sybil.GetTable(*sybil.Flags.Table)

	for scanner.Scan() {
		line := scanner.Text()
		r := t.NewRecord()
		fields := strings.Split(line, ",")
		for i, v := range fields {
			field_name := header_fields[i]

			if v == "" {
				continue
			}

			val, err := strconv.ParseFloat(v, 64)
			if err == nil {
				r.AddIntField(field_name, int64(val))
			} else {
				r.AddStrField(field_name, v)
			}

		}

		t.ChunkAndSave()
	}

}

func json_query(obj *interface{}, path []string) []interface{} {

	var ret interface{}
	ret = *obj

	for _, key := range path {
		if key == "$" {
			continue
		}

		switch ing := ret.(type) {
		case map[string]interface{}:
			ret = ing[key]
		case []interface{}:
			// the key should be an integer key...
			intkey, err := strconv.ParseInt(key, 10, 32)
			if err != nil {
				sybil.Debug("USING NON INTEGER KEY TO ACCESS ARRAY!", key, err)
			} else {
				ret = ing[intkey]
			}
		case nil:
			continue
		default:
			sybil.Debug(fmt.Sprintf("DONT KNOW HOW TO ADDRESS INTO OBJ %T", ing))
		}

	}

	switch r := ret.(type) {
	case []interface{}:
		return r
	case map[string]interface{}:
		ret := make([]interface{}, 0)
		ret = append(ret, r)
		return ret
	default:
		sybil.Debug(fmt.Sprintf("RET TYPE %T", r))
	}

	return nil
}

func import_json_records() {
	t := sybil.GetTable(*sybil.Flags.Table)

	path := strings.Split(JSON_PATH, ".")
	sybil.Debug("PATH IS", path)

	dec := json.NewDecoder(os.Stdin)

	for {
		var decoded interface{}

		if err := dec.Decode(&decoded); err != nil {
			if err == io.EOF {
				break
			}
			if err != nil {
				sybil.Debug("ERR", err)
			}
		}

		records := json_query(&decoded, path)
		decoded = nil

		for _, ing := range records {
			r := t.NewRecord()
			switch dict := ing.(type) {
			case map[string]interface{}:
				ndict := Dictionary(dict)
				ingestDirecotry(r, &ndict, "")
			case Dictionary:
				ingestDirecotry(r, &dict, "")

			}
			t.ChunkAndSave()
		}

	}

}

var INT_CAST = make(map[string]bool)
var EXCLUDES = make(map[string]bool)

func runIngestCmdLine() {
	ingestfile := flag.String("file", sybil.IngestDir, "name of dir to ingest into")
	f_INTS := flag.String("ints", "", "columns to treat as ints (comma delimited)")
	f_CSV := flag.Bool("csv", false, "expect incoming data in CSV format")
	f_EXCLUDES := flag.String("exclude", "", "Columns to exclude (comma delimited)")
	f_JSON_PATH := flag.String("path", "$", "Path to JSON record, ex: $.foo.bar")
	f_SKIP_COMPACT := flag.Bool("skip-compact", false, "skip auto compaction during ingest")
	f_REOPEN := flag.String("infile", "", "input file to use (instead of stdin)")
	sybil.Flags.SkipCompact = f_SKIP_COMPACT

	flag.Parse()

	digestfile := fmt.Sprintf("%s", *ingestfile)

	if *sybil.Flags.Table == "" {
		flag.PrintDefaults()
		return
	}

	JSON_PATH = *f_JSON_PATH

	if *f_REOPEN != "" {

		infile, err := os.OpenFile(*f_REOPEN, syscall.O_RDONLY|syscall.O_CREAT, 0666)
		if err != nil {
			sybil.Error("ERROR OPENING INFILE", err)
		}

		os.Stdin = infile

	}

	if *sybil.Flags.Profile {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	for _, v := range strings.Split(*f_INTS, ",") {
		INT_CAST[v] = true
	}
	for _, v := range strings.Split(*f_EXCLUDES, ",") {
		EXCLUDES[v] = true
	}

	for k, _ := range EXCLUDES {
		sybil.Debug("EXCLUDING COLUMN", k)
	}

	t := sybil.GetTable(*sybil.Flags.Table)

	// We have 5 tries to load table info, just in case the lock is held by
	// someone else
	var loaded_table = false
	for i := 0; i < TABLE_INFO_GRABS; i++ {
		loaded := t.LoadTableInfo()
		if loaded == true || t.HasFlagFile() == false {
			loaded_table = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if loaded_table == false {
		if t.HasFlagFile() {
			sybil.Warn("INGESTOR COULDNT READ TABLE INFO, LOSING SAMPLES")
			return
		}
	}

	if *f_CSV == false {
		import_json_records()
	} else {
		import_csv_records()
	}

	t.IngestRecords(digestfile)
}
