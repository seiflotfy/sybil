package main

import sybil "github.com/logv/sybil/pkg"
import "flag"
import "strings"

func runIndexCmdLine() {
	var fInts = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *sybil.Flags.Table == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *fInts != "" {
		ints = strings.Split(*fInts, *sybil.Flags.FieldSeparator)
	}

	sybil.Flags.UpdateTableInfo = &trueFlag

	t := sybil.GetTable(*sybil.Flags.Table)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.DeleteBlocksAfterQuery = true
	sybil.Opts.WriteBlockInfo = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
