//+build profile

package pkg

import "github.com/pkg/profile"

var PROFILER_ENABLED = true
var Profile ProfilerStart

type PkgProfile struct {
}

func (p PkgProfile) Start() ProfilerStart {
	if *Flags.ProfileMem {
		Profile = profile.Start(profile.MemProfile, profile.ProfilePath("."))
	} else {
		Profile = profile.Start(profile.CPUProfile, profile.ProfilePath("."))
	}
	return Profile
}
func (p PkgProfile) Stop() {
	p.Stop()
}

func STOP_PROFILER() {
	Profile.Stop()
}

var RUN_PROFILER = func() ProfilerStop {
	Debug("RUNNING ENABLED PROFILER")
	return PkgProfile{}
}
