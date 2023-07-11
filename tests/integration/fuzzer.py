from network_sandbox import Network
from types import SimpleNamespace
import random
import json
import requests

class DefaultMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        new_trace = []
        for e in trace:
            new_trace.append(e)
        return new_trace
    
class TLCGuider:
    def __init__(self, tlc_addr) -> None:
        self.tlc_addr = tlc_addr
        self.states = {}
    
    def check_new_state(self, trace, event_trace):
        trace_to_send = event_trace
        trace_to_send.append({"reset": True})
        r = requests.post("http://"+self.tlc_addr+"/execute", json=json.dumps(trace_to_send))
        if r.ok:
            response = r.json()
            have_new = False
            for i in range(len(response["states"])):
                tlc_state = {"state": response["states"][i], "key" : response["keys"][i]}
                if tlc_state["key"] not in self.states:
                    self.states[tlc_state["key"]] = tlc_state
                    have_new = True
            return have_new
        return False
    

class Fuzzer:
    def __init__(self, cluster, config = {}) -> None:
        self.config = self._validate_config(config)
        self.network = Network(self.config.network_addr)
        self.guider = self.config.guider
        self.mutator = self.config.mutator
        self.cluster = cluster
        self.trace_queue = []

    def _validate_config(self, config):
        new_config = SimpleNamespace()
        if "mutator" not in config:
            new_config.mutator = DefaultMutator()
        else:
            new_config.mutator = config["mutator"]
        
        if "network_addr" not in config:
            new_config.network_addr = ("127.0.0.1", 7074)
        else:
            new_config.network_addr = config["network_addr"]
        
        if "guider" not in config:
            tlc_addr = "127.0.0.1:2023"
            if "tlc_addr" in config:
                tlc_addr = config["tlc_addr"]
            new_config.guider = TLCGuider(tlc_addr)
        else:
            new_config.guider = config["guider"]

        if "no_iterations" not in config:
            new_config.no_iterations = 100
        else:
            new_config.no_iterations = 100

        if "horizon" not in config:
            new_config.horizon = 50
        else:
            new_config.horizon = config["horizon"]

        if "nodes" not in config:
            new_config.nodes = 3
        else:
            new_config.nodes = config["nodes"]
        
        if "crash_quota" not in config:
            new_config.crash_quota = 4
        else:
            new_config.crash_quota = config["crash_quota"]

        if "mutations_per_trace" not in config:
            new_config.mutations_per_trace = 5
        else:
            new_config.mutations_per_trace = config["mutations_per_trace"]
        
        if "init_population" not in config:
            new_config.init_population = 5
        else:
            new_config.init_population = config["init_population"]
        
        if "test_harness" not in config:
            new_config.test_harness = 5
        else:
            new_config.test_harness = config["test_harness"]

        return new_config

    def run(self):
        self.network.run()

        for i in range(self.config.init_population):
            (trace, event_trace) = self.run_iteration()
            for j in range(self.config.mutations_per_trace):
                self.trace_queue.append(self.mutator.mutate(trace))

        for i in range(self.config.no_iterations):
            to_mimic = None
            if len(self.trace_queue) > 0:
                to_mimic = self.trace_queue.pop(0)
            (trace, event_trace) = self.run_iteration(to_mimic)
            if self.guider.check_new_state(trace, event_trace):
                for j in range(self.config.mutations_per_trace):
                    self.trace_queue.append(self.mutator.mutate(trace))

        # TODO: plot coverage

        self.network.shutdown()

    def run_iteration(self, mimic = None):
        trace = []
        crashed = None

        crash_points = {}
        schedule = []
        client_requests = []
        if mimic is None:
            node_ids = list(range(1, self.config.nodes+1))
            for c in random.sample(range(0, self.config.horizon, 2), self.config.crash_quota):
                crash_points[c] = random.choice(node_ids)

            client_requests = random.sample(range(self.config.horizon), self.config.test_harness)
            
            for i in range(self.config.horizon):
                schedule.append(random.choice(node_ids))
        else:
            schedule = [0 for i in range(self.config.horizon)]
            for ch in mimic:
                if ch["type"] == "Crash":
                    crash_points[ch["node"]] = ch["step"]
                elif ch["type"] == "Schedule":
                    schedule[ch["step"]] = ch["node"]
                elif ch["type"] == "ClientRequest":
                    client_requests.append(ch["step"])


        self.cluster.create(self.config.nodes)
        for i in range(self.config.horizon):
            if crashed is not None:
                self.cluster.node(crashed).start()
                self.network.add_event({"name": "Add", "params": {"i": crashed}})
                crashed = None
            
            if i in crash_points:
                node_id = crash_points[i]
                crashed = node_id
                self.cluster.node(node_id).terminate()
                trace.append({"type": "Crash", "node": node_id, "step": i})
                self.network.add_event({"name": "Remove", "params": {"i": node_id}})
            
            for node_id in self.cluster.node_ids():
                state = self.cluster.node(node_id).info()['raft_role']
                self.network.add_event({"name": "UpdateState", "params": {"state": state}})
            
            self.network.schedule_replica(schedule[i])
            trace.append({"type": "Schedule", "node": schedule[i], "step": i})

            if i in client_requests:
                assert self.cluster.execute('INCRBY', 'counter', 1) == i + 1
                trace.append({"type": "ClientRequest", "step": i})
                
        
        assert int(self.cluster.execute('GET', 'counter')) == self.config.horizon
        self.cluster.destroy()

        event_trace = self.network.get_event_trace()
        self.network.clear_mailboxes()

        return (trace, event_trace)
        