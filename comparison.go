package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
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
	c.plot()

	return nil
}

func (c *Comparison) plot() {
	p := plot.New()
	p.Title.Text = "Comparison"
	p.X.Label.Text = "Iteration"
	p.Y.Label.Text = "States covered"

	k := 0
	for benchmark, stats := range c.Stats {
		plotPoints := make([]plotter.XY, len(stats.Coverages))
		for i, c := range stats.Coverages {
			plotPoints[i] = plotter.XY{
				X: float64(i),
				Y: float64(c),
			}
		}
		line, err := plotter.NewLine(plotter.XYs(plotPoints))
		if err != nil {
			continue
		}
		line.Color = plotutil.Color(k)
		p.Add(line)
		p.Legend.Add(benchmark, line)

		k++
	}
	plotFile := path.Join(c.FuzzerConfig.RecordPath, "coverage.png")
	p.Save(4*vg.Inch, 4*vg.Inch, plotFile)

	bs, err := json.Marshal(c.Stats)
	if err != nil {
		return
	}
	dataFile := path.Join(c.FuzzerConfig.RecordPath, "data.json")
	os.WriteFile(dataFile, bs, 0644)
}
