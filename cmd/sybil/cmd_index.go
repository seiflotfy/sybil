package main

import sybil "github.com/logv/sybil/pkg"
import "flag"
import "strings"

func RunIndexCmdLine() {
	var f_INTS = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *f_INTS != "" {
		ints = strings.Split(*f_INTS, *sybil.FLAGS.FIELD_SEPARATOR)
	}

	sybil.FLAGS.UPDATE_TABLE_INFO = &TRUE

	t := sybil.GetTable(*sybil.FLAGS.TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.DELETE_BLOCKS_AFTER_QUERY = true
	sybil.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
