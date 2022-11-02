"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""

import time
import os
import os.path
import subprocess
import threading
import itertools
import random
import logging
import signal
import uuid
import shutil
import redis

LOG = logging.getLogger('sandbox')


class RedisRaftSanitizer(Exception):
    pass


class RedisRaftError(Exception):
    pass


class RedisRaftTimeout(RedisRaftError):
    pass


class RedisRaftFailedToStart(RedisRaftError):
    pass


class PipeLogger(threading.Thread):
    def __init__(self, pipe, prefix):
        super(PipeLogger, self).__init__()
        self.prefix = prefix
        self.pipe = pipe
        self.daemon = True
        self.loglines = ""
        self.start()

    def run(self):
        try:
            for line in iter(self.pipe.readline, b''):
                linestr = str(line, 'utf-8').rstrip()
                self.loglines += linestr
                LOG.debug('%s: %s', self.prefix, linestr)
        except ValueError as err:
            LOG.debug("PipeLogger: %s", str(err))
            return


class RedisRaft(object):
    def __init__(self, _id, port, config, redis_args=None, raft_args=None,
                 use_id_arg=True, cluster_id=0, password=None, tls_ca_cert_location=None):
        self.id = _id
        self.cluster_id = cluster_id
        self.guid = str(uuid.uuid4())
        self.port = port
        self.executable = config.executable
        self.process = None
        self.paused = False
        self.workdir = os.path.abspath(config.workdir)
        self.serverdir = os.path.join(self.workdir, self.guid)
        self._raftlog = 'redis{}.db'.format(self.id)
        self._raftlogidx = '{}.idx'.format(self.raftlog)
        self._dbfilename = 'redis{}.rdb'.format(self.id)
        self.up_timeout = config.up_timeout
        self.keepfiles = config.keepfiles
        self.args = config.args.copy() if config.args else []

        self.args += ['--port', str(0) if config.tls else str(port),
                      '--bind', '0.0.0.0',
                      '--dir', self.serverdir,
                      '--dbfilename', self._dbfilename,
                      '--loglevel', config.raft_loglevel]
        if password:
            self.args += ['--requirepass', password]

        self.cacert = os.getcwd() + '/tests/tls/ca.crt'
        self.cacert_dir = os.getcwd() + '/tests/tls'
        self.cert = os.getcwd() + '/tests/tls/redis.crt'
        self.key = os.getcwd() + '/tests/tls/redis.key'

        if config.tls:
            if tls_ca_cert_location == 'dir' or tls_ca_cert_location == 'both':
                self.args += ['--tls-ca-cert-dir', self.cacert_dir]
            # default behaviour - only file configuration
            if not tls_ca_cert_location or tls_ca_cert_location == 'file' or tls_ca_cert_location == 'both':
                self.args += ['--tls-ca-cert-file', self.cacert]
            self.args += ['--tls-port', str(port),
                          '--tls-cert-file', self.cert,
                          '--tls-key-file', self.key,
                          '--tls-key-file-pass', 'redisraft']

        self.args += redis_args if redis_args else []
        self.args += ['--loadmodule', os.path.abspath(config.raftmodule)]

        if raft_args is None:
            raft_args = {}
        else:
            raft_args = raft_args.copy()

        if password:
            raft_args['cluster-password'] = password

        if use_id_arg:
            raft_args['id'] = str(_id)

        default_args = {'addr': self.address,
                        'log-filename': self._raftlog,
                        'log-fsync': 'yes' if config.fsync else 'no',
                        'loglevel': config.raft_loglevel,
                        'tls-enabled': 'yes' if config.tls else 'no'}

        for defkey, defval in default_args.items():
            if defkey not in raft_args:
                raft_args[defkey] = defval

        raft_args = {'--raft.' + k: v for k, v in raft_args.items()}
        self.raft_args = [str(x) for x in
                          itertools.chain.from_iterable(raft_args.items())]

        client_cacert = os.getcwd() + '/tests/tls/ca.crt'
        client_cert = os.getcwd() + '/tests/tls/client.crt'
        client_key = os.getcwd() + '/tests/tls/client.key'

        self.client = redis.Redis(host='localhost', port=self.port,
                                  socket_timeout=20,
                                  password=password,
                                  ssl=config.tls,
                                  ssl_certfile=client_cert,
                                  ssl_keyfile=client_key,
                                  ssl_ca_certs=client_cacert)

        self.client.connection_pool.connection_kwargs['parser_class'] = \
            redis.connection.PythonParser
        self.client.set_response_callback('info raft', redis.client.parse_info)
        self.client.set_response_callback('config get',
                                          redis.client.parse_config_get)
        self.stdout = None
        self.stderr = None
        self.cleanup()

    @property
    def address(self):
        return 'localhost:{}'.format(self.port)

    @property
    def raftlog(self):
        return os.path.join(self.serverdir, self._raftlog)

    @property
    def raftlogidx(self):
        return os.path.join(self.serverdir, self._raftlogidx)

    @property
    def dbfilename(self):
        return os.path.join(self.serverdir, self._dbfilename)

    def cluster(self, *args, single_run=False):
        retries = self.up_timeout
        if retries is not None:
            retries *= 10
        if single_run:
            retries = 1
        while True:
            try:
                return self.client.execute_command('RAFT.CLUSTER', *args)
            except redis.exceptions.RedisError as err:
                LOG.info(err)
                if retries is not None:
                    retries -= 1
                    if retries <= 0:
                        LOG.fatal('RAFT.CLUSTER %s failed', " ".join(args))
                        raise err
                time.sleep(0.1)

    def init(self, cluster_id=None):
        self.cleanup()
        self.start()

        if cluster_id is None:
            dbid = self.cluster('init')
        else:
            dbid = self.cluster('init', cluster_id)

        LOG.info('Cluster created: %s', dbid)
        return self

    def join(self, addresses, single_run=False):
        self.start()
        self.cluster('join', *addresses, single_run=single_run)
        return self

    def start(self, extra_raft_args=None, verify=True):
        try:
            os.makedirs(self.serverdir)
        except OSError:
            pass

        if extra_raft_args is None:
            extra_raft_args = []
        args = [self.executable] + self.args + self.raft_args + extra_raft_args
        logging.info("starting node: args = {}".format(args))

        self.process = subprocess.Popen(
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            executable=self.executable,
            args=args)

        self.stdout = PipeLogger(self.process.stdout,
                                 'c{}/n{}/stdout'.format(self.cluster_id,
                                                         self.id))
        self.stderr = PipeLogger(self.process.stderr,
                                 'c{}/n{}/stderr'.format(self.cluster_id,
                                                         self.id))

        if not verify:
            return
        self.verify_up()
        LOG.info('RedisRaft<%s> is up, pid=%s, guid=%s', self.id,
                 self.process.pid, self.guid)

    def load_module(self, module):
        self.verify_up()
        self.client.execute_command("module", "load", f'{os.getcwd()}/tests/integration/modules/{module}')

    def unload_module(self, module):
        self.verify_up()
        self.client.execute_command("module", "unload", module)

    def process_is_up(self):
        if not self.process:
            return False

        self.process.poll()
        return self.process.returncode is None

    def verify_down(self, retries=50, retry_delay=0.1):
        while retries > 0:
            if not self.process_is_up():
                return True
            time.sleep(retry_delay)
            retries -= 1
        return False

    def verify_up(self):
        retries = self.up_timeout
        if retries is not None:
            retries *= 10
        while True:
            try:
                self.client.ping()
                return
            except redis.exceptions.ConnectionError:
                if retries is not None:
                    retries -= 1
                    if not retries:
                        LOG.fatal('RedisRaft<%s> failed to start', self.id)
                        raise RedisRaftFailedToStart(
                            'RedisRaft<%s> failed to start' % self.id)
                time.sleep(0.1)

    def check_sanitizer_error(self):
        if (self.stdout and 'sanitizer' in self.stdout.loglines.lower() or
                self.stderr and 'sanitizer' in self.stderr.loglines.lower()):
            raise RedisRaftSanitizer('Sanitizer error.')

    def terminate(self):
        if self.process:
            if self.paused:
                self.resume()
            try:
                self.process.terminate()

                try:
                    self.process.communicate(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill()

            except OSError as err:
                LOG.error('RedisRaft<%s> failed to terminate: %s',
                          self.id, err)
            else:
                LOG.info('RedisRaft<%s> terminated', self.id)

        if self.stdout:
            self.stdout.join(timeout=30)
        if self.stderr:
            self.stderr.join(timeout=30)

        self.process = None
        self.check_sanitizer_error()

    def kill(self):
        if self.process:
            try:
                self.process.kill()

                try:
                    self.process.communicate(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill()

            except OSError as err:
                LOG.error('Cannot kill RedisRaft<%s>: %s',
                          self.id, err)
            else:
                LOG.info('RedisRaft<%s> killed', self.id)
        self.process = None
        self.check_sanitizer_error()

    def restart(self, retries=5):
        self.terminate()
        while retries > 0:
            try:
                self.start()
                break
            except RedisRaftFailedToStart:
                retries -= 1
                time.sleep(0.5)
                continue

    def pause(self):
        if self.process is not None:
            self.paused = True
            self.process.send_signal(signal.SIGSTOP)

    def resume(self):
        if self.process is not None:
            self.paused = False
            self.process.send_signal(signal.SIGCONT)

    def cleanup(self):
        if not self.keepfiles:
            shutil.rmtree(self.serverdir, ignore_errors=True)

    def execute(self, *cmd):
        return self.client.execute_command(*cmd)

    def config_set(self, key, val):
        return self.client.config_set(key, val)

    def config_get(self, key):
        return self.client.config_get(key)

    def info(self):
        return self.client.execute_command('info raft')

    def raft_debug_exec(self, *cmd):
        """
        Execute the specified Redis command through RAFT.DEBUG EXEC,
        so it executes locally and does not go through Raft interception.
        """

        return self.client.execute_command('raft.debug', 'exec', *cmd)

    def commit_index(self):
        return self.info()['raft_commit_index']

    def current_index(self):
        return self.info()['raft_current_index']

    def transfer_leader(self, *args, **kwargs):
        return self.client.execute_command('raft.transfer_leader',
                                           *args, **kwargs)

    def timeout_now(self):
        return self.client.execute_command('raft.timeout_now')

    @staticmethod
    def _wait_for_condition(test_func, timeout_func, timeout=3):
        retries = timeout * 10
        while retries > 0:
            try:
                if test_func():
                    return
            except redis.ConnectionError:
                pass

            retries -= 1
            time.sleep(0.1)
        timeout_func()

    def wait_for_election(self, timeout=10):
        def has_leader():
            return bool(self.info()['raft_leader_id'] != -1)

        def raise_no_master_error():
            raise RedisRaftTimeout('No master elected')
        self._wait_for_condition(has_leader, raise_no_master_error, timeout)

    def wait_for_log_committed(self, timeout=10):
        def current_idx_committed():
            info = self.info()
            return bool(info['raft_commit_index'] == info['raft_current_index'])

        def raise_not_committed():
            raise RedisRaftTimeout('Last log entry not yet committed')
        self._wait_for_condition(current_idx_committed, raise_not_committed,
                                 timeout)
        LOG.debug("Finished waiting for latest entry to be committed.")

    def wait_for_log_applied(self, timeout=10):
        def commit_idx_applied():
            info = self.info()
            commit = info['raft_commit_index']
            last_applied = info['raft_last_applied_index']
            return bool(commit == last_applied)

        def raise_not_applied():
            raise RedisRaftTimeout('Last committed entry not yet applied')
        self._wait_for_condition(commit_idx_applied, raise_not_applied,
                                 timeout)
        LOG.debug("Finished waiting logs to be applied.")

    def wait_for_current_index(self, idx, timeout=10):
        def current_idx_reached():
            info = self.info()
            return bool(info['raft_current_index'] == idx)

        def raise_not_reached():
            info = self.info()
            LOG.debug("------- last info before bail out: %s\n", info)

            raise RedisRaftTimeout(
                'Expected current index %s not reached' % idx)
        self._wait_for_condition(current_idx_reached, raise_not_reached,
                                 timeout)

    def wait_for_commit_index(self, idx, gt_ok=False, timeout=10):
        def commit_idx_reached():
            info = self.info()
            if gt_ok:
                return bool(info['raft_commit_index'] >= idx)
            return bool(info['raft_commit_index'] == idx)

        def raise_not_reached():
            info = self.info()
            LOG.debug("------- last info before bail out: %s\n", info)

            raise RedisRaftTimeout(
                'Expected commit index %s not reached' % idx)
        self._wait_for_condition(commit_idx_reached, raise_not_reached,
                                 timeout)

    def wait_for_num_voting_nodes(self, count, timeout=10):
        def num_voting_nodes_match():
            info = self.info()
            return bool(info['raft_num_voting_nodes'] == count)

        def raise_not_added():
            raise RedisRaftTimeout('Nodes not added')

        self._wait_for_condition(num_voting_nodes_match, raise_not_added,
                                 timeout)
        LOG.debug("Finished waiting for num_voting_nodes == %d", count)

    def wait_for_num_nodes(self, count, timeout=10):
        def num_nodes_match():
            info = self.info()
            return bool(info['raft_num_nodes'] == count)

        def raise_not_added():
            raise RedisRaftTimeout('Nodes count did not modify')

        self._wait_for_condition(num_nodes_match, raise_not_added, timeout)
        LOG.debug("Finished waiting for num_nodes == %d", count)

    def wait_for_node_voting(self, value='yes', timeout=10):
        def check_voting():
            info = self.info()
            return bool(info['raft_is_voting'] == value)

        def raise_not_voting():
            info = self.info()
            LOG.debug("Non voting node: %s", str(info))
            raise RedisRaftTimeout('Node voting != %s' % value)

        self._wait_for_condition(check_voting, raise_not_voting, timeout)

    def wait_for_info_param(self, name, value, timeout=10):
        def check_param():
            info = self.info()
            return bool(info.get(name) == value)

        def raise_not_matched():
            raise RedisRaftTimeout('INFO "%s" did not reach "%s"' %
                                   (name, value))

        self._wait_for_condition(check_param, raise_not_matched, timeout)

    def destroy(self):
        try:
            self.terminate()
        finally:
            self.cleanup()


class Cluster(object):
    noleader_timeout = 30

    def __init__(self, config, base_port=5000, base_id=0, cluster_id=0):
        self.next_id = base_id + 1
        self.cluster_id = cluster_id
        self.base_port = base_port
        self.nodes = {}
        self.leader = None
        self.raft_args = None
        self.config = config

    def nodes_count(self):
        return len(self.nodes)

    def node_ids(self):
        return self.nodes.keys()

    def node_ports(self):
        return [n.port for n in self.nodes.values()]

    def node_addresses(self):
        return [n.address for n in self.nodes.values()]

    def create(self, node_count, raft_args=None, cluster_id=None, password=None,
               prepopulate_log=0, tls_ca_cert_location=None):
        if raft_args is None:
            raft_args = {}
        self.raft_args = raft_args.copy()
        assert self.nodes == {}
        self.nodes = {x: RedisRaft(x, self.base_port + x,
                                   config=self.config,
                                   raft_args=raft_args,
                                   cluster_id=self.cluster_id,
                                   password=password,
                                   tls_ca_cert_location=tls_ca_cert_location)
                      for x in range(1, node_count + 1)}
        self.next_id = node_count + 1
        for _id, node in self.nodes.items():
            if _id == 1:
                node.init(cluster_id=cluster_id)
            else:
                logging.info("{} joining".format(_id))
                node.join(['localhost:{}'.format(self.base_port + 1)])

        self.leader = 1
        self.node(1).wait_for_num_voting_nodes(len(self.nodes))
        self.wait_for_unanimity()

        # Pre-populate if asked
        for _ in range(prepopulate_log):
            assert self.execute('INCR', 'log-prepopulate-key')

        return self

    def config_set(self, param, value):
        for node in self.nodes.values():
            node.client.config_set(param, value)

    def add_initialized_node(self, node):
        self.nodes[node.id] = node

    def add_node(self, raft_args=None, port=None, cluster_setup=True,
                 node_id=None, use_cluster_args=False, single_run=False,
                 join_addr_list=None, redis_args=None, tls_ca_cert_location=None,**kwargs):
        _raft_args = raft_args
        if use_cluster_args:
            _raft_args = self.raft_args
        _id = self.next_id if node_id is None else node_id
        self.next_id += 1
        if port is None:
            port = self.base_port + _id
        node = None
        try:
            node = RedisRaft(_id, port, self.config, redis_args,
                             raft_args=_raft_args, tls_ca_cert_location=tls_ca_cert_location, **kwargs)
            if cluster_setup:
                if self.nodes:
                    if join_addr_list is None:
                        join_addr_list = self.node_addresses()
                    node.join(join_addr_list, single_run=single_run)
                else:
                    node.init()
                    self.leader = _id
            self.nodes[_id] = node
            return node
        except redis.exceptions.RedisError:
            node.kill()
            raise

    def reset_leader(self):
        self.leader = next(iter(self.nodes.keys()))

    def remove_node(self, _id):
        def _func():
            self.node(self.leader).client.execute_command(
                'RAFT.NODE', 'REMOVE', _id)
        try:
            self.raft_retry(_func)
        except redis.ResponseError as err:
            # If we are removing the leader, leader will shutdown before sending
            # the reply. On retry, we should get "node id does not exist" reply.
            if str(err).startswith("node id does not exist"):
                if _id not in self.nodes:
                    raise err

        self.nodes[_id].destroy()
        del self.nodes[_id]
        if self.leader == _id:
            self.reset_leader()

    def random_node_id(self):
        return random.choice(list(self.nodes.keys()))

    def find_node_id_by_port(self, port):
        for node in self.nodes.values():
            if node.port == port:
                return node.id
        return None

    def node(self, _id):
        return self.nodes[_id]

    def random_node(self):
        return self.nodes[self.random_node_id()]

    def leader_node(self):
        return self.nodes[self.leader]

    def update_leader(self):
        def _func():
            # This command will be redirected to the leader in raft_retry
            self.node(self.leader).client.execute_command('get x')
        self.raft_retry(_func)

    def wait_for_unanimity(self, exclude=None):
        commit_idx = self.node(self.leader).commit_index()
        for _id, node in self.nodes.items():
            if exclude is not None and int(_id) in exclude:
                continue
            node.wait_for_commit_index(commit_idx, gt_ok=True)
            node.wait_for_log_applied()

    def wait_for_replication(self, exclude=None):
        current_idx = self.node(self.leader).current_index()
        for _id, node in self.nodes.items():
            if exclude is not None and int(_id) in exclude:
                continue
            node.wait_for_current_index(current_idx)

    def raft_retry(self, func):
        start_time = time.time()
        while time.time() < start_time + self.noleader_timeout:
            try:
                return func()
            except redis.ConnectionError:
                self.leader = self.random_node_id()
            except redis.ReadOnlyError:
                time.sleep(0.5)
            except redis.ResponseError as err:
                if str(err).startswith('READONLY'):
                    # While loading a snapshot we can get a READONLY
                    time.sleep(0.5)
                if str(err).startswith('UNBLOCKED'):
                    # Ignore unblocked replies...
                    time.sleep(0.5)
                elif str(err).startswith('MOVED'):
                    start_time = time.time()
                    port = int(str(err).split(':')[-1])
                    new_leader = self.find_node_id_by_port(port)
                    assert new_leader is not None
                    assert new_leader != self.leader

                    # When removing a leader there can be a race condition,
                    # in this case we need to do nothing
                    if new_leader in self.nodes:
                        self.leader = new_leader
                elif str(err).startswith('CLUSTERDOWN') or \
                        str(err).startswith('NOCLUSTER'):
                    remaining = start_time + self.noleader_timeout - time.time()
                    if remaining > 0:
                        LOG.info("-CLUSTERDOWN response received, will retry"
                                 " for %.2f seconds", remaining)
                    time.sleep(0.5)
                else:
                    raise
        raise RedisRaftError('No leader elected')

    def execute(self, *cmd):
        """
        Execute the specified command on the leader node; Handle redirects
        and retries as necessary.
        """

        def _func():
            return self.nodes[self.leader].client.execute_command(*cmd)
        return self.raft_retry(_func)

    def destroy(self):
        err = None

        for node in self.nodes.values():
            try:
                node.destroy()
            except RedisRaftSanitizer as e:
                err = e
        if err:
            raise err

    def terminate(self):
        err = None
        for node in self.nodes.values():
            try:
                node.terminate()
            except RedisRaftSanitizer as e:
                err = e
        if err:
            raise err

    def start(self):
        for node in self.nodes.values():
            node.start()

    def restart(self):
        for node in self.nodes.values():
            node.terminate()
        for node in self.nodes.values():
            node.start()


def assert_after(func, timeout, retry_interval=0.5):
    """
    Call func() which is expected to perform certain assertions.
    If assertions failed, retry with a retry_interval delay until
    timeout has been reached -- at which point an exception is raised.
    """
    start_time = time.time()
    while True:
        try:
            func()
            break
        except AssertionError:
            if time.time() > start_time + timeout:
                raise
            time.sleep(retry_interval)
