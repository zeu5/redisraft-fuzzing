
import requests
import logging
import json
from os import path
from hashlib import sha256

LOG = logging.getLogger('fuzzer')

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

        return 0
    
    def coverage(self):
        return len(self.states.keys())

    def reset(self):
        self.states = {}

def create_event_graph(event_trace):
    cur_event = {}
    nodes = {}

    for e in event_trace:
        replica = e["params"]["replica"]
        node = {"name": e["name"], "params": e["params"], "replica": replica}
        if replica in cur_event:
            node["prev"] = cur_event[replica]["id"]
        id = sha256(json.dumps(node, sort_keys=True).encode('utf-8')).hexdigest()
        node["id"] = id

        cur_event[replica] = node
        nodes[id] = node
    
    return nodes

class TraceGuider(TLCGuider):
    def __init__(self, tlc_addr, record_path) -> None:
        super(TraceGuider, self).__init__(tlc_addr, record_path)
        self.traces = {}

    def check_new_state(self, trace, event_trace, name, record=False) -> int:
        super().check_new_state(trace, event_trace, name, record)

        new = 0
        event_graph = create_event_graph(event_trace)
        event_graph_id = sha256(json.dumps(event_graph, sort_keys=True).encode('utf-8')).hexdigest()

        if event_graph_id not in self.traces:
            self.traces[event_graph_id] = True
            new = 1
        
        return new
    
    def reset(self):
        self.traces = {}
        return super().reset()