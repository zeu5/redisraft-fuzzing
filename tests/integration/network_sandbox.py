from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit, parse_qs
from threading import Lock, Thread
import requests
import json

class Request:
    def __init__(self, method, path, headers, query=None, content=None) -> None:
        self.path = path
        self.headers = headers
        self.query = query
        self.content = content
        self.method = method

class Response:
    def __init__(self, status_code, content) -> None:
        self.status_code = status_code
        self.headers = {}
        self.content = content

    def set_header(self, key, value):
        self.headers[key] = value

    def set_content_json(self):
        self.headers["Content-Type"] = "application/json"

    def json(status_code, content):
        r = Response(status_code, content)
        r.set_content_json()
        return r


class _ServerHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, handler = None, **kwargs) -> None:
        self.handler = handler
        super().__init__(*args, **kwargs)

    def respond(self, response: Response):
        if response is None:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Failed to generate a response")

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            for key in response.headers:
                self.send_header(key, response.headers[key])
            self.send_error(response.status_code, response.content)
        else:
            self.send_response(response.status_code)
            for key in response.headers:
                self.send_header(key, response.headers[key])

            content = None
            if response.content is not None and len(response.content) > 0:
                content = response.content.encode("UTF-8", "replace")
            
            if content is None:
                self.send_header("Content-Length", 0)
                self.end_headers()
            else:
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            return

    def do_GET(self):
        (_, _, path, query, _) = urlsplit(self.path)
        parsed_qs = parse_qs(query)
        request = Request("GET", path, self.headers, parsed_qs)
        response = self.handler.handle(request)
        self.respond(response)

    def do_POST(self):
        (_, _, path, query, _) = urlsplit(self.path)
        parsed_qs = parse_qs(query)
        length = self.headers.get('content-length')
        content = ""
        if int(length) > 0:
            content = self.rfile.read(int(length))
        
        request = Request("POST", path, self.headers, parsed_qs, content)
        response = self.handler.handle(request)
        self.respond(response)


class Router:
    def __init__(self) -> None:
        self.handlers = {}
    
    def add_route(self, path, handler):
        self.handlers[path] = handler

    def handle(self, request):
        if request.path in self.handlers:
            response = self.handlers[request.path](request)
            return response
        return Response(HTTPStatus.NOT_FOUND, "Path does not exist")
    

class Server(ThreadingHTTPServer):
    def __init__(self, 
                 server_address, 
                 handler: Router, 
                 bind_and_activate: bool = True) -> None:
        self.handler = handler
        super().__init__(server_address, _ServerHandler, bind_and_activate)

    def finish_request(self, request, client_address) -> None:
        if self.RequestHandlerClass == _ServerHandler:
            return self.RequestHandlerClass(request, client_address, self, handler=self.handler)
        return super().finish_request(request, client_address)
    

class Message:
    def __init__(self, fr, to, type, msg, id=None) -> None:
        self.fr = fr
        self.to = to
        self.type = type
        self.msg = msg
        self.id = id

    def from_str(str):
        m = json.loads(str)
        if "from" not in m or "to" not in m or "type" not in m or "data" not in m:
            return None
        return Message(m["from"], m["to"], m["type"], m["data"], m["id"] if "id" in m else None)


class Network:
    def __init__(self, addr) -> None:
        self.addr = addr
        self.lock = Lock()
        self.mailboxes = {}
        self.replicas = {}
        self.event_trace = []

        router = Router()
        router.add_route("/replica", self._handle_replica)
        router.add_route("/message", self._handle_message)

        self.server = Server(addr, router)
        self.server_thread = Thread(target=self.server.serve_forever)

    def run(self):
        self.server_thread.start()
    
    def shutdown(self):
        self.server.shutdown()
        self.server_thread.join()
    
    def _handle_replica(self, request: Request) -> Response:
        replica = json.loads(request.content)
        if "id" in replica:
            try:
                self.lock.acquire()
                self.replicas[replica["id"]] = replica
            finally:
                self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))

    def _handle_message(self, request: Request) -> Response:
        msg = Message.from_str(request.content)
        if msg is not None:
            try:
                self.lock.acquire()
                if msg.to not in self.mailboxes:
                    self.mailboxes[msg.to] = []
                self.mailboxes[msg.to].append(msg)
            finally:
                self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))
    
    def _handle_event(self, request: Request) -> Response:
        event = json.loads(request.content)
        if "replica" in event:
            try:
                e = {"name": event["type"], "params": event["params"]}
                e["params"]["replica"] = event["replica"]
                self.lock.acquire()
                self.event_trace.append(e)
            finally:
                self.lock.release()

        return Response.json(HTTPStatus.OK, json.dumps({"message": "Ok"}))
    
    def get_replicas(self):
        replicas = []
        try:
            self.lock.acquire()
            replicas = list(self.replicas.items())
        finally:
            self.lock.release()
        return replicas
    
    def get_event_trace(self):
        event_trace = []
        try:
            self.lock.acquire()
            for e in self.event_trace:
                event_trace.append(e)
        finally:
            self.lock.release()
        return event_trace
    
    def add_event(self, e):
        try:
            self.lock.acquire()
            self.event_trace.append(e)
        finally:
            self.lock.release()
    
    def schedule_replica(self, replica):
        addr = ""
        messages_to_deliver = []
        try:
            self.lock.acquire()
            if replica in self.mailboxes and len(self.mailboxes[replica]) > 0:
                for m in self.mailboxes[replica]:
                    messages_to_deliver.append(m)
                self.mailboxes[replica] = []
                addr = self.replicas[replica]["addr"]
        finally:
            self.lock.release()

        for next_msg in messages_to_deliver:
            requests.post("http://"+addr+"/message", json=json.dumps(next_msg))

    def clear_mailboxes(self):
        try:
            self.lock.acquire()
            for key in self.mailboxes:
                self.mailboxes[key] = []
            self.event_trace = []
        finally:
            self.lock.release()