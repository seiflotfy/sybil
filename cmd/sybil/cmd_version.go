package main

import sybil "github.com/logv/sybil/pkg"
import "flag"

func runVersionCmdLine() {
	sybil.Flags.JSON = flag.Bool("json", false, "Print results in JSON format")
	flag.Parse()

	sybil.PrintVersionInfo()

}
