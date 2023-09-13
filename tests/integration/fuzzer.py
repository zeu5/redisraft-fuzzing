from .network_sandbox import Network
from .sandbox import RedisRaftBug, Cluster
from .fuzzer_guider import TLCGuider
from types import SimpleNamespace
from os import path, makedirs
from threading import Lock, Thread, Event
import random
import json
import logging
import time

LOG = logging.getLogger('fuzzer')

class DefaultMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        new_trace = []
        for e in trace:
            new_trace.append(e)
        return new_trace

class RandomMutator():
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        return None
    
class SwapMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []
        max_step = 0
        for e in trace:
            if e["type"] == "Schedule" and e["step"] > max_step:
                max_step = e["step"]
        
        [first, second] = random.sample(range(max_step), 2)
        first_value = ()
        second_value = ()
        for e in trace:
            if e["type"] == "Schedule" and e["step"] == first:
                first_value = {"type": "Schedule", "node": e["node"], "step": e["step"], "max_messages": e["max_messages"]}
            elif e["type"] == "Schedule" and e["step"] == second:
                second_value = {"type": "Schedule", "node": e["node"], "step": e["step"], "max_messages": e["max_messages"]}
        
        for e in trace:
            if e["type"] != "Schedule":
                new_trace.append(e)
            if e["step"] == first:
                new_trace.append(second_value)
            elif e["step"] == second:
                new_trace.append(first_value)
            else:
                new_trace.append(e)
        
        return new_trace
    
class SwapCrashStepsMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []

        crash_steps = set()
        for e in trace:
            if e["type"] == "Crash":
                crash_steps.add(e["step"])
        
        [first, second] = random.sample(list(crash_steps), 2)
        for e in trace:
            if e["type"] != "Crash":
                new_trace.append(e)
            
            if e["step"] == first:
                new_trace.append({"type": "Crash", "node": e["node"], "step": second})
            elif e["step"] == second:
                new_trace.append({"type": "Crash", "node": e["node"], "step": first})
            else:
                new_trace.append(e)
            
        return new_trace
    
class SwapCrashNodesMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []

        crash_steps = {}
        for e in trace:
            if e["type"] == "Crash":
                crash_steps[e["step"]] = e["node"]
        
        [first, second] = random.sample(list(crash_steps.keys()), 2)
        for e in trace:
            if e["type"] != "Crash":
                new_trace.append(e)
            
            if e["step"] == first:
                new_trace.append({"type": "Crash", "node":crash_steps[second], "step": e["step"]})
            elif e["step"] == second:
                new_trace.append({"type": "Crash", "node":crash_steps[first], "step": e["step"]})
            else:
                new_trace.append(e)

        return new_trace


class CombinedMutator:
    def __init__(self, mutators) -> None:
        self.mutators = mutators
    
    def mutate(self, trace: list[dict]) -> list[dict]:
        new_trace = []
        for e in trace:
            new_trace.append(e)
        
        for m in self.mutators:
            new_trace = m.mutate(new_trace)
        
        return new_trace


# Encode all the calls that the workers need to synchronize on
# The main fuzzer class will create and pass this to all workers
class _Fuzzer_Synchronizer:
    def __init__(self, config) -> None:
        self.lock = Lock()
        self.trace_queue = []
        self.config = config
        self.guider = config.guider
        self.mutator = config.mutator
        self.stats = {
            "coverage" : [0],
            "random_traces": 0,
            "mutated_traces": 0
        }
        self.iteration = 0
        self.start_time = None
        self.logger = LOG.getChild("sync")
    
    def update_mutator(self, name, mutator):
        self.logger.info("Updating mutator")
        self.mutator = mutator
        self.config.record_file_prefix = name

    def get_trace(self):
        # Need to return iteration number and trace to mimic
        # If done with all iterations then return None to stop the worker
        # Else if trace queue is empty, then create a new random trace and send

        iteration = 0
        with self.lock:
            iteration = self.iteration
        
        if iteration == self.config.iterations:
            return ("", None)
        elif iteration % self.config.seed_frequency == 0:
            # Keep counter for number of iterations and reseed
            self.seed()

        to_mimic = None
        if len(self.trace_queue) > 0:
            to_mimic = self.trace_queue.pop(0)
        
        if to_mimic is None:
            to_mimic = self.get_random_trace()
            with self.lock:
                self.stats["random_traces"] += 1
        else:
            with self.lock:
                self.stats["mutated_traces"] += 1

        with self.lock:
            self.iteration +=1 

        self.logger.info("Starting iteration: {}".format(iteration+1))
        return (str(iteration+1), to_mimic)

    def record_logs(self, record_path, logs):
        with open(record_path, "w") as record_file:
            record_file.writelines(logs)
        
    
    def update_iteration(self, iteration, trace, event_trace, logs):
        if len(logs) != 0:
            record_file_path = path.join(self.config.report_path, "{}_{}.log".format(self.config.record_file_prefix, iteration))
            self.record_logs(record_file_path, logs)

        new_states = self.guider.check_new_state(trace, event_trace, str(iteration), record=False)
        if new_states > 0:
            for j in range(new_states * self.config.mutations_per_trace):
                mutated_trace = self.mutator.mutate(trace)
                if mutated_trace is not None:
                    self.logger.debug("Adding mutated trace")
                    with self.lock:
                        self.trace_queue.append(mutated_trace)

    def get_iteration_no(self):
        iteration = None
        with self.lock:
            iteration = self.iteration
        return iteration

    def reset(self):
        self.guider.reset()
        with self.lock:
            self.trace_queue = []
            self.stats = {
                "coverage" : [0],
                "random_traces": 0,
                "mutated_traces": 0
            }
            self.iteration = 0
    
    def get_stats(self):
        stats = {}
        with self.lock:
            stats = {
                "coverage": self.stats["coverage"],
                "random_traces": self.stats["random_traces"],
                "mutated_traces": self.stats["mutated_traces"],
                "runtime": self.stats["runtime"],
            }
        return stats

    def get_random_trace(self):
        crash_points = {}
        start_points = {}
        schedule = []
        node_ids = list(range(1, self.config.nodes+1))
        for c in random.sample(range(0, self.config.horizon, 2), self.config.crash_quota):
            node_id = random.choice(node_ids)
            crash_points[c] = node_id
            s = random.choice(range(c, self.config.horizon))
            start_points[s] = node_id

        client_requests = {}
        for c in random.sample(range(self.config.horizon), self.config.test_harness):
            client_requests[c] = random.choice(["read", "write"])
        for choice in random.choices(node_ids, k=self.config.horizon):
            max_messages = random.randint(0, self.config.max_messages_to_schedule)
            schedule.append((choice, max_messages))

        crashed = set()
        trace = []
        for j in range(self.config.horizon):
            if j in start_points and start_points[j] in crashed:
                node_id = start_points[j]
                trace.append({"type": "Start", "node": node_id, "step": j})
                crashed.remove(node_id)
            if j in crash_points:
                node_id = crash_points[j]
                trace.append({"type": "Crash", "node": node_id, "step": j})
                crashed.add(node_id)
            if j in client_requests:
                trace.append({"type": "ClientRequest", "step": j, "op": client_requests[j]})

            trace.append({"type": "Schedule", "node": schedule[j][0], "step": j, "max_messages": schedule[j][1]})
        return [e for e in trace]
    
    def seed(self):       
        self.logger.info("Seeding")
        new_traces = []
        for i in range(self.config.seed_population):            
            new_traces.append(self.get_random_trace())
        
        with self.lock:
            self.trace_queue = new_traces

        self.logger.info("Finished seeding")

    def record_start(self):
        self.start_time = time.time_ns()

    def record_end(self):
        self.stats["runtime"] = time.time_ns() - self.start_time

# A worker thread
# 1. Waits for a trace from the synchronizer
# 2. Cluster/interceptor setup
# 3. Run
# 4. Cluster/interceptor teardown
# 5. Synchronize
class _Fuzzer_Worker:
    def __init__(self, sync: _Fuzzer_Synchronizer, num, cluster_factory, config=SimpleNamespace()) -> None:
        self.sync = sync
        self.num = num
        self.cluster_factory = cluster_factory
        self.config = config
        (addr, port) = self.config.network_addr
        self.intercept_addr=(addr, port+num)
        self.network = Network(self.intercept_addr)
        self._stop_event = Event()
        self.logger = LOG.getChild("Worker {}".format(num))

    def stop(self):
        self._stop_event.set()
    
    def shutdown(self):
        self.logger.info("Shuting down")
        self.network.shutdown()

    def start(self):
        self.logger.info("Starting")
        self.network.run()
    
    def get_cluster(self) -> Cluster:
        base_port = 5000+self.num*10
        return self.cluster_factory(True, base_port=base_port, cluster_id=self.num, intercept_addr=self.intercept_addr)

    def run_with(self, iteration, mimic):

        logger = self.logger.getChild("iteration {}".format(iteration))

        crashed = set()
        crash_points = {}
        start_points = {}
        schedule = []
        client_requests = {}

        schedule = [(1, random.randint(0, self.config.max_messages_to_schedule)) for i in range(self.config.horizon)]
        for ch in mimic:
            if ch["type"] == "Crash":
                crash_points[ch["step"]] = ch["node"]
            elif ch["type"] == "Start":
                start_points[ch["step"]] = ch["node"]
            elif ch["type"] == "Schedule":
                schedule[ch["step"]] = (ch["node"], ch["max_messages"])
            elif ch["type"] == "ClientRequest":
                client_requests[ch["step"]] = ch["op"]
        
        logger.info("iteration: {}: Creating cluster".format(self.num, iteration))
        cluster = self.get_cluster()
        cluster.create(self.config.nodes, wait=False)
        self.network.wait_for_nodes(self.config.nodes)

        trace = []
        logs = []

        def get_logs(cluster):
            lines = []
            for _id, logs in cluster.get_log_lines().items():
                lines.append("Log for node: {}\n".format(_id))
                lines.append("------ Stdout -----\n")
                for line in logs["stdout"]:
                    lines.append(line+"\n")
                lines.append("------ Stderr -----\n")
                for line in logs["stderr"]:
                    lines.append(line+"\n")
                lines.append("\n\n")
            return lines

        try:
            for i in range(self.config.horizon):
                if self._stop_event.is_set():
                    break

                logger.debug("Taking step {}".format(i))
                if i in start_points and start_points[i] in crashed:
                    node_id = start_points[i]
                    logger.info("Starting crashed node")
                    cluster.node(node_id).start(verify=False)
                    trace.append({"type": "Start", "node": node_id, "step": i})
                    self.network.add_event({"name": "Add", "params": {"i": node_id}})
                    crashed.remove(node_id)
                
                if i in crash_points:
                    logger.info("Crashing node")
                    node_id = crash_points[i]
                    crashed.add(node_id)
                    if node_id in cluster.node_ids():
                        cluster.node(node_id).terminate(check_error=False)
                    trace.append({"type": "Crash", "node": node_id, "step": i})
                    self.network.add_event({"name": "Remove", "params": {"i": node_id}})

                self.network.schedule_replica(schedule[i][0], schedule[i][1])
                trace.append({"type": "Schedule", "node": schedule[i][0], "step": i, "max_messages": schedule[i][1]})

                if i in client_requests:
                    try:
                        logger.info("Executing client request")
                        if client_requests[i] == "read":
                            cluster.execute_async('GET')
                        else:
                            cluster.execute_async('INCRBY', 'counter', 1, with_retry=False)
                    except:
                        pass
                    trace.append({"type": "ClientRequest", "step": i})

                time.sleep(0.005)
        except:
            logs = get_logs(cluster)
        finally:
            logger.debug("Destroying cluster")
            try:
                cluster.destroy()
            except:
                logs = get_logs(cluster)

        event_trace = self.network.get_event_trace()
        self.network.clear_mailboxes()
    
        self.sync.update_iteration(iteration, trace, event_trace, logs)

    def run(self):
        self._stop_event = Event()
        (iteration, mimic) = self.sync.get_trace()
        while mimic is not None:
            if self._stop_event.is_set():
                break
            LOG.info("Worker: {}, iteration: {}".format(self.num, iteration))
            self.run_with(iteration, mimic)
            (iteration, mimic) = self.sync.get_trace()

        self.stop()
        

class Fuzzer:
    def __init__(self, cluster_factory, config = {}) -> None:
        self.config = self._validate_config(config)
        self.sync = _Fuzzer_Synchronizer(self.config)
        self.cluster_factor = cluster_factory
        self.workers = [_Fuzzer_Worker(self.sync, i, self.cluster_factor, self.config) for i in range(0, self.config.num_workers)]

        self.worker_threads = []

    def get_stats(self):
        return self.sync.get_stats()

    def update_mutator(self, name, mutator):
        self.sync.update_mutator(name, mutator)
    
    def reset(self):
        if len(self.worker_threads) != 0:
            # Need to check if workers are started before stopping
            for w in self.workers:
                w.stop()

        self.sync.reset()
        self.worker_threads = [Thread(target=w.run) for w in self.workers]

    def _validate_config(self, config):
        new_config = SimpleNamespace()
        
        if "mutator" not in config:
            new_config.mutator = DefaultMutator()
        else:
            if config["mutator"] == "swap":
                new_config.mutator = SwapMutator()
            else:
                new_config.mutator = DefaultMutator()
            
        
        if "network_addr" not in config:
            new_config.network_addr = ("127.0.0.1", 7074)
        else:
            new_config.network_addr = config["network_addr"]

        if "iterations" not in config:
            new_config.iterations = 10
        else:
            new_config.iterations = config["iterations"]

        if "horizon" not in config:
            new_config.horizon = 100
        else:
            new_config.horizon = config["horizon"]

        if "nodes" not in config:
            new_config.nodes = 3
        else:
            new_config.nodes = config["nodes"]
        
        if "crash_quota" not in config:
            new_config.crash_quota = 10
        else:
            new_config.crash_quota = config["crash_quota"]

        if "mutations_per_trace" not in config:
            new_config.mutations_per_trace = 3
        else:
            new_config.mutations_per_trace = config["mutations_per_trace"]
        
        if "seed_population" not in config:
            new_config.seed_population = 50
        else:
            new_config.seed_population = config["seed_population"]

        new_config.seed_frequency = 1000
        if "seed_frequency" in config:
            new_config.seed_frequency = config["seed_frequency"]
        
        if "test_harness" not in config:
            new_config.test_harness = 3
        else:
            new_config.test_harness = config["test_harness"]

        if "max_message_to_schedule" not in config:
            new_config.max_messages_to_schedule = 5
        else:
            new_config.max_messages_to_schedule = config["max_messages_to_schedule"]

        report_path = "fuzzer_report"
        if "report_path" in config:
            report_path = config["report_path"]
        
        makedirs(report_path, exist_ok=True)
        new_config.report_path= report_path

        new_config.record_file_prefix = ""
        if "record_file_prefix" in config:
            new_config.record_file_prefix = config["record_file_prefix"]

        tlc_record_path = path.join(new_config.report_path, "traces" if new_config.record_file_prefix == "" else new_config.record_file_prefix+"_traces")
        makedirs(tlc_record_path, exist_ok=True)
        tlc_addr = "127.0.0.1:2023"
        if "tlc_addr" in config:
            tlc_addr = config["tlc_addr"]
        new_config.guider = TLCGuider(tlc_addr, tlc_record_path)

        new_config.num_workers = 1
        if "worker_threads" in config:
            new_config.num_workers = config["worker_threads"]

        return new_config
    
    def start(self):
        LOG.info("Starting fuzzer with {} workers".format(self.config.num_workers))
        for w in self.workers:
            w.start()

    def shutdown(self):
        for w in self.workers:
            w.shutdown()

    def run(self):
        self.sync.record_start()

        for t in self.worker_threads:
            t.run()

        while True:
            # Poll the sync if it is done
            if self.sync.get_iteration_no() == self.config.iterations:
                break
            time.sleep(0.05)

        for w in self.workers:
            w.stop()
        
        self.sync.record_end()
        
    
    def record_stats(self):
        stats = self.sync.get_stats()
        cov_path = path.join(self.config.report_path, "stats.json" if self.config.record_file_prefix == "" else self.config.record_file_prefix+"_stats.json")
        with open(cov_path, "w") as cov_file:
            json.dump(stats, cov_file)