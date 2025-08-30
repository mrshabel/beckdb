.PHONY: test bench bench-memory

# run all test suites
test:
	go test ./... -v -race

# benchmark application performance
bench:
	go test -bench=.

# benchmark memory profiling
bench-memory:
	go test -bench=. -benchmem

bench-server:
	redis-benchmark -p 6379 -t set,get -n 100000 -c 50 -d 256