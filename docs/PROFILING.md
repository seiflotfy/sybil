profiling
---------

    make profile

    # ADD Profile FLAG
    ./bin/sybil query -profile -table test0 -group age -int age
    go tool pprof ./bin/sybil cpu.pprof

    python scripts/fakedata/host_generator.py 10000 | ./bin/sybil ingest -profile -table test0
    go tool pprof ./bin/sybil cpu.pprof

    # Profile MEMORY
    ./bin/sybil query -profile -table test0 -group age -int age -mem
    go tool pprof ./bin/sybil mem.pprof

