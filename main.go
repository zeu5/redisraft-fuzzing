package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"

	"github.com/spf13/cobra"
)

func MainCommand() *cobra.Command {
	var episodes int
	var horizon int
	var savePath string
	var workPath string
	var nodes int
	var requests int
	var workers int
	var logLevel string
	var runs int

	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			fConfig := FuzzerConfig{
				Iterations:      episodes,
				Horizon:         horizon,
				NumWorkers:      workers,
				NumNodes:        nodes,
				RecordPath:      savePath,
				BaseNetworkPort: 7074,
				BaseWorkingDir:  workPath,
				LogLevel:        logLevel,

				MutationsPerTrace: 3,
				SeedPopulation:    20,
				NumRequests:       requests,
				NumCrashes:        10,
				MaxMessages:       5,
				ReseedFrequency:   200,

				ClusterConfig: &ClusterConfig{
					NumNodes:              nodes,
					RaftModulePath:        "/home/snagendra/Fuzzing/redisraft-fuzzing/redisraft.so",
					RedisServerBinaryPath: "/home/snagendra/Fuzzing/redis/src/redis-server",
				},
			}
			mutator := CombineMutators(NewSwapCrashNodeMutator(2), NewSwapNodeMutator(20), NewSwapMaxMessagesMutator(20))

			c := NewComparison(fConfig)
			c.AddBenchmark("line", NewLineCovGuider("/home/snagendra/Fuzzing/redisraft-fuzzing/build/deps/raft/CMakeFiles/raft.dir/src", "127.0.0.1:2023", path.Join(savePath, "line_traces")), mutator)
			c.AddBenchmark("random", NewTLCStateGuider("127.0.0.1:2023", path.Join(savePath, "random_traces")), RandomMutator())
			c.AddBenchmark("tlc", NewTLCStateGuider("127.0.0.1:2023", path.Join(savePath, "tlc_traces")), mutator)
			c.AddBenchmark("trace", NewTraceCoverageGuider("127.0.0.1:2023", path.Join(savePath, "trace_traces")), mutator)

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt)

			doneCh := make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				select {
				case <-sigCh:
				case <-doneCh:
				}
				cancel()
			}()

			fmt.Println("Running...")
			c.Run(ctx, runs)
			fmt.Println("Completed.")
			close(doneCh)
		},
	}
	cmd.PersistentFlags().IntVarP(&episodes, "episodes", "e", 1000, "Number of iterations")
	cmd.PersistentFlags().IntVar(&horizon, "horizon", 100, "Number of steps for fuzzer to take")
	cmd.PersistentFlags().IntVarP(&nodes, "nodes", "n", 3, "Number of nodes")
	cmd.PersistentFlags().IntVar(&runs, "runs", 3, "Number of runs")
	cmd.PersistentFlags().IntVarP(&requests, "requests", "r", 3, "Number of requests")
	cmd.PersistentFlags().IntVarP(&workers, "workers", "w", 1, "Number of workers")
	cmd.PersistentFlags().StringVarP(&savePath, "save", "s", "results", "Path to save coverage comparisons")
	cmd.PersistentFlags().StringVar(&workPath, "work-path", "tmp", "Work directory to store the temporary files for redis server")
	cmd.PersistentFlags().StringVarP(&logLevel, "log-level", "v", "info", "Log level")
	return cmd
}

func main() {
	cmd := MainCommand()
	cmd.Execute()
}
