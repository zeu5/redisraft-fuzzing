package main

import (
	"context"
	"fmt"
)

type benchmark struct {
	name    string
	guider  Guider
	mutator Mutator
}

type Stats struct {
	Coverages     []int
	RandomTraces  int
	MutatedTraces int
}

func NewStats() *Stats {
	return &Stats{
		Coverages: make([]int, 0),
	}
}

func (s *Stats) Copy() *Stats {
	new := &Stats{
		Coverages:     make([]int, len(s.Coverages)),
		RandomTraces:  s.RandomTraces,
		MutatedTraces: s.MutatedTraces,
	}
	copy(new.Coverages, s.Coverages)

	return new
}

func (s *Stats) AddCoverage(c int) {
	s.Coverages = append(s.Coverages, c)
}

type Comparison struct {
	Benchmarks []benchmark
	Stats      map[string]*Stats

	FuzzerConfig FuzzerConfig
}

func NewComparison(fc FuzzerConfig) *Comparison {
	return &Comparison{
		Benchmarks:   make([]benchmark, 0),
		Stats:        make(map[string]*Stats),
		FuzzerConfig: fc,
	}
}

func (c *Comparison) AddBenchmark(name string, guider Guider, mutator Mutator) {
	c.Benchmarks = append(c.Benchmarks, benchmark{
		name:    name,
		guider:  guider,
		mutator: mutator,
	})
	c.Stats[name] = nil
}

func (c *Comparison) Run(ctx context.Context) error {
	fuzzer, err := NewFuzzer(ctx, c.FuzzerConfig)
	if err != nil {
		return err
	}

	for _, b := range c.Benchmarks {
		fmt.Printf("Benchmark: %s\n", b.name)
		fuzzer.Reset(b.name, b.guider, b.mutator)
		fuzzer.Run()
		c.Stats[b.name] = fuzzer.GetStats()
	}

	// TODO: record and plot
	for benchmark, stats := range c.Stats {
		cov := stats.Coverages[len(stats.Coverages)-1]
		fmt.Printf("Coverage for %s: %d\n", benchmark, cov)
	}

	return nil
}
