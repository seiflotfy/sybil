package main

import sybil "github.com/logv/sybil/pkg"
import "flag"
import "strings"

func runIndexCmdLine() {
	var fInts = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *fInts != "" {
		ints = strings.Split(*fInts, *sybil.FLAGS.FIELD_SEPARATOR)
	}

	sybil.FLAGS.UPDATE_TABLE_INFO = &trueFlag

	t := sybil.GetTable(*sybil.FLAGS.TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.DeleteBlocksAfterQuery = true
	sybil.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
