package main

import "flag"

import sybil "github.com/logv/sybil/pkg"

func runDigestCmdLine() {
	flag.Parse()

	if *sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *sybil.FLAGS.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	sybil.DELETE_BLOCKS_AFTER_QUERY = false

	t := sybil.GetTable(*sybil.FLAGS.TABLE)
	if t.LoadTableInfo() == false {
		sybil.Warn("Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords()
}
