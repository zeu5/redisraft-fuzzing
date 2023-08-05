from .network_sandbox import Network
from .sandbox import RedisRaftBug
from types import SimpleNamespace
from os import path, makedirs
import random
import json
import requests
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
    
class TLCGuider:
    def __init__(self, tlc_addr, record_path) -> None:
        self.tlc_addr = tlc_addr
        self.record_path = record_path
        self.states = {}
    
    def check_new_state(self, trace, event_trace, name, record = False) -> int:
        trace_to_send = event_trace
        trace_to_send.append({"reset": True})
        LOG.debug("Sending trace to TLC: {}".format(trace_to_send))
        try:
            r = requests.post("http://"+self.tlc_addr+"/execute", json=trace_to_send)
            if r.ok:
                response = r.json()
                LOG.debug("Received response from TLC: {}".format(response))               
                new_states = 0
                for i in range(len(response["states"])):
                    tlc_state = {"state": response["states"][i], "key" : response["keys"][i]}
                    if tlc_state["key"] not in self.states:
                        self.states[tlc_state["key"]] = tlc_state
                        new_states += 1
                if record:
                    with open(path.join(self.record_path, name+".log"), "w") as record_file:
                        lines = ["Trace sent to TLC: \n", json.dumps(trace_to_send, indent=2)+"\n\n", "Response received from TLC:\n", json.dumps(response, indent=2)+"\n"]
                        record_file.writelines(lines)
                return new_states
            else:
                LOG.info("Received error response from TLC, code: {}, text: {}".format(r.status_code, r.content))
        except Exception as e:
            LOG.info("Error received from TLC: {}".format(e))
            pass
        finally:
            return 0
    
    def coverage(self):
        return len(self.states.keys())

    def reset(self):
        self.states = {}

class Fuzzer:
    def __init__(self, cluster_factory, config = {}) -> None:
        self.config = self._validate_config(config)
        LOG.info("Intercept network address: {addr}".format(addr=self.config.network_addr))
        self.network = Network(self.config.network_addr)
        self.guider = self.config.guider
        self.mutator = self.config.mutator
        self.cluster_factory = cluster_factory
        self.trace_queue = []
        self.stats = {
            "coverage" : [0],
            "random_traces": 0,
            "mutated_traces": 0
        }
    
    def reset(self):
        self.network.clear_mailboxes()
        self.guider.reset()
        self.trace_queue = []
        self.stats = {
            "coverage" : [0],
            "random_traces": 0,
            "mutated_traces": 0
        }

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
            new_config.horizon = 50
        else:
            new_config.horizon = config["horizon"]

        if "nodes" not in config:
            new_config.nodes = 3
        else:
            new_config.nodes = config["nodes"]
        
        if "crash_quota" not in config:
            new_config.crash_quota = 8
        else:
            new_config.crash_quota = config["crash_quota"]

        if "mutations_per_trace" not in config:
            new_config.mutations_per_trace = 5
        else:
            new_config.mutations_per_trace = config["mutations_per_trace"]
        
        if "seed_population" not in config:
            new_config.seed_population = 10
        else:
            new_config.seed_population = config["seed_population"]

        new_config.seed_frequency = 100
        if "seed_frequency" in config:
            new_config.seed_frequency = config["seed_frequency"]
        
        if "test_harness" not in config:
            new_config.test_harness = 3
        else:
            new_config.test_harness = config["test_harness"]

        if "max_message_to_schedule" not in config:
            new_config.max_messages_to_schedule = 6
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

        return new_config

    def seed(self, iter):
        LOG.info("Seeding for iteration {}".format(iter))
        self.trace_queue = []
        for i in range(self.config.seed_population):
            (trace, _) = self.run_iteration("seed_{}_{}".format(iter, i))
            self.trace_queue.append(trace)

    def run(self):
        LOG.info("Starting fuzzer loop")
        for i in range(self.config.iterations):
            LOG.info("Starting fuzzer iteration %d", i)
            if i % self.config.seed_frequency == 0:
                self.seed(i)

            to_mimic = None
            if len(self.trace_queue) > 0:
                to_mimic = self.trace_queue.pop(0)
            if to_mimic is None:
                self.stats["random_traces"] += 1
            else:
                self.stats["mutated_traces"] += 1
            try:
                (trace, event_trace) = self.run_iteration("fuzz_{}".format(i), to_mimic)
            except Exception as ex:
                LOG.info("Error running iteration %d: %s", i, ex)
            else:
                trace_name = str(i) if self.config.record_file_prefix == "" else "{}_{}".format(self.config.record_file_prefix, i)
                new_states = self.guider.check_new_state(trace, event_trace, trace_name, record=False)
                if new_states > 0:
                    for j in range(new_states * self.config.mutations_per_trace):
                        mutated_trace = self.mutator.mutate(trace)
                        if mutated_trace is not None:
                            self.trace_queue.append(mutated_trace)
                self.stats["coverage"].append(self.guider.coverage())
    
    def record_stats(self):
        cov_path = path.join(self.config.report_path, "stats.json" if self.config.record_file_prefix == "" else self.config.record_file_prefix+"_stats.json")
        with open(cov_path, "w") as cov_file:
            json.dump(self.stats, cov_file)

    def run_iteration(self, iteration, mimic = None):
        trace = []
        crashed = set()

        crash_points = {}
        start_points = {}
        schedule = []
        client_requests = []
        if mimic is None:
            node_ids = list(range(1, self.config.nodes+1))
            for c in random.sample(range(0, self.config.horizon, 2), self.config.crash_quota):
                node_id = random.choice(node_ids)
                crash_points[c] = node_id
                s = random.choice(range(c, self.config.horizon))
                start_points[s] = node_id

            client_requests = random.sample(range(self.config.horizon), self.config.test_harness)
            for choice in random.choices(node_ids, k=self.config.horizon):
                max_messages = random.randint(0, self.config.max_messages_to_schedule)
                schedule.append((choice, max_messages))
        else:
            schedule = [(1, random.randint(0, self.config.max_messages_to_schedule)) for i in range(self.config.horizon)]
            for ch in mimic:
                if ch["type"] == "Crash":
                    crash_points[ch["step"]] = ch["node"]
                elif ch["type"] == "Start":
                    start_points[ch["step"]] = ch["node"]
                elif ch["type"] == "Schedule":
                    schedule[ch["step"]] = (ch["node"], ch["max_messages"])
                elif ch["type"] == "ClientRequest":
                    client_requests.append(ch["step"])

        def record_logs(record_path, cluster):
            LOG.debug("Recording logs to path: {}".format(record_path))
            with open(record_path, "w") as record_file:
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
                record_file.writelines(lines)


        cluster = self.cluster_factory(True)

        LOG.debug("Creating cluster")
        cluster.create(self.config.nodes, wait=False)
        self.network.wait_for_nodes(self.config.nodes)
        event_trace = []
        try:
            for i in range(self.config.horizon):
                LOG.debug("Taking step {}".format(i))
                if i in start_points and start_points[i] in crashed:
                    node_id = start_points[i]
                    LOG.debug("Starting crashed node")
                    cluster.node(node_id).start()
                    trace.append({"type": "Start", "node": node_id, "step": i})
                    self.network.add_event({"name": "Add", "params": {"i": node_id}})
                    crashed.remove(node_id)
                
                if i in crash_points:
                    LOG.debug("Crashing node")
                    node_id = crash_points[i]
                    crashed.add(node_id)
                    if node_id in cluster.node_ids():
                        cluster.node(node_id).terminate()
                    trace.append({"type": "Crash", "node": node_id, "step": i})
                    self.network.add_event({"name": "Remove", "params": {"i": node_id}})
                                
                for node_id in cluster.node_ids():
                    LOG.debug("Updating state of node: {}".format(node_id))
                    if node_id not in crashed:
                        info = cluster.node(node_id).info()
                        state = info['raft_role']
                        lastlog_index = int(info["raft_current_index"])
                        commit_index = int(info["raft_commit_index"])
                        term = int(info["raft_current_term"])
                        self.network.add_event({"name": "UpdateState", "params": {"i": node_id, "state": state, "last_index": lastlog_index, "commit_index": commit_index, "term": term}})

                self.network.schedule_replica(schedule[i][0], schedule[i][1])
                trace.append({"type": "Schedule", "node": schedule[i][0], "step": i, "max_messages": schedule[i][1]})

                if i in client_requests:
                    try:
                        LOG.debug("Executing client request")                        
                        cluster.execute_async('INCRBY', 'counter', 1, with_retry=False)
                    except:
                        pass
                    trace.append({"type": "ClientRequest", "step": i})

                time.sleep(0.005)
        except:
            record_logs(path.join(self.config.report_path, "{}_{}.log".format(self.config.record_file_prefix, iteration)), cluster)
        finally:
            LOG.debug("Destroying cluster")
            try:
                cluster.destroy()
            except:
                record_logs(path.join(self.config.report_path, "{}_{}.log".format(self.config.record_file_prefix, iteration)), cluster)

        event_trace = self.network.get_event_trace()
        self.network.clear_mailboxes()

        return (trace, event_trace)