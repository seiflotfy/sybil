package main

import "flag"

import sybil "github.com/logv/sybil/pkg"

func runDigestCmdLine() {
	flag.Parse()

	if *sybil.Flags.Table == "" {
		flag.PrintDefaults()
		return
	}

	if *sybil.Flags.Profile {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	sybil.DeleteBlocksAfterQuery = false

	t := sybil.GetTable(*sybil.Flags.Table)
	if t.LoadTableInfo() == false {
		sybil.Warn("Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords()
}
