.PHONY: test bench bench-memory

# run all test suites
test:
	go test ./...

# benchmark application performance
bench:
	go test -bench=.

# benchmark memory profiling
bench-memory:
	go test -bench=. -benchmem