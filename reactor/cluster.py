"""
===============
RAFT Leadership
===============
The raft module is to be used for cluster leadership elections. ``RaftNode`` provides no shared state or log between
node neighbours, nor does it persist any state. As such it should be used only for managing leadership elections.

``RaftNode`` is multi-threaded and designed to be used in the same process that needs to know if it is a leader::

    import threading
    from reactor.cluster import RaftNode

    raft_node = RaftNode(address=('hostname.of.neighbour2', 7000)
                         neighbours=[('hostname.of.neighbour2', 7000), ('hostname.of.neighbour3', 7000)])
    raft_thread = threading.Thread(target=raft_node.execute, name='RAFT-execute')
    raft_thread.start()


    ....

    raft_node.shutdown()
    raft_thread.join()


It is important for the hostname in ``RaftNode.address`` to be the same as the address given to the neighbours list.
If they do not match then communication between neighbours will fail, i.e.::

    # This would fail as node address and neighbour address do not match
    raft_node1 = RaftNode(('', 7000), neighbours=[('node2.example.com',7000), ('node3.example.com',7000)]
    raft_node2 = RaftNode(('', 7000), neighbours=[('node1.example.com',7000), ('node3.example.com',7000)]
    raft_node3 = RaftNode(('', 7000), neighbours=[('node3.example.com',7000), ('node1.example.com',7000)]

    # This would work as node address and neighbour address match
    raft_node1 = RaftNode(('node1.example.com', 7000),
                          neighbours=[('node2.example.com',7000), ('node3.example.com',7000)]
    raft_node2 = RaftNode(('node2.example.com', 7000),
                          neighbours=[('node1.example.com',7000), ('node3.example.com',7000)]
    raft_node3 = RaftNode(('node3.example.com', 7000),
                          neighbours=[('node3.example.com',7000), ('node1.example.com',7000)]


``RaftNode`` supports SSL communication between nodes. To be used it requires a key and certificate for each
node and a shared CA::

    ...
    # This must be set before execute
    raft_node.set_ssl('/path/to/node.key',
                      '/path/to/node.crt',
                      '/path/to/ca.pem')
    raft_node.execute()



``RaftNode`` uses ``logging.getLogger('raft')``.

Meta data can be stored in a ``RaftNode`` which is then shared with neighbouring nodes in every message sent. Meta data
should, for this reason, be kept to a minimum and used only to share important, required information.
"""
import json
import logging
import math
import os
import queue
import random
import socket
import ssl
import statistics
import struct
import threading
import time
from collections import OrderedDict, deque
from typing import Dict, Optional

# By default add the null handler to the library logger
logging.getLogger('raft').addHandler(logging.NullHandler())

STATE_ORPHAN = 1
STATE_FOLLOWER = 2
STATE_CANDIDATE = 3
STATE_LEADER = 4

ELECTION_TIMEOUT = 0.5
HEARTBEAT_TIMEOUT = 0.1

RPC_PING = 1
RPC_APPEND_ENTRIES = 2
RPC_VOTE_ELECTION = 3
RPC_CLUSTER_JOIN = 4
RPC_CLUSTER_LEAVE = 5

MEMBERSHIP_JOINING = 1
MEMBERSHIP_ACTIVE = 2
MEMBERSHIP_LEAVING = 3

MSG_HEADER_FORMAT = '>H'


_rpcToName = {
    RPC_PING:           'ping',
    RPC_APPEND_ENTRIES: 'append',
    RPC_VOTE_ELECTION:  'vote',
    RPC_CLUSTER_JOIN:   'join',
    RPC_CLUSTER_LEAVE:  'leave',
}


def _next_timeout(raft_state: int, exponent: int, election_timeout: float, heartbeat_timeout: float) -> float:
    """ Calculates the next timeout time based on the `raft_state` and timeout times. """
    if raft_state == STATE_FOLLOWER or raft_state == STATE_ORPHAN:
        return time.time() + random.uniform(election_timeout, 2 * election_timeout)
    elif raft_state == STATE_CANDIDATE:
        election_timeout *= 2 ** exponent
        return time.time() + random.uniform(election_timeout, 2 * election_timeout)
    elif raft_state == STATE_LEADER:
        heartbeat_timeout *= 2 ** exponent
        return time.time() + heartbeat_timeout


def _rpc_request(rpc: int, recipient) -> dict:
    """
    Creates a request RPC.

    :param rpc: Procedure being requested
    :param recipient: Recipient of the message
    """
    return {'id': random.getrandbits(16),
            'term': None,
            'rpc': rpc,
            'recipient': recipient}


def _rpc_response(msg: dict, response: any) -> dict:
    """
    Creates a response RPC.

    :param msg: RPC request message
    :param response: Response to the request
    """
    return {'id': msg['id'],
            'term': None,
            'rpc': -msg['rpc'],
            'recipient': msg['sender'],
            'response': response}


def _rpc_str(msg: dict) -> str:
    """ Generate a string of the RPC `msg`. """
    msg_str = [
        f"#{msg['id']:>5}",
        f"term={msg['term']:>2}",
        f"rpc={_rpcToName[abs(msg['rpc'])]:6s} {'req' if msg['rpc'] > 0 else 'res'}",
        f"recipient={msg['recipient']}",
        f"sender={msg['sender']}"
    ]
    # If the RPC is a response
    if msg['rpc'] < 0:
        msg_str.append(f"response={msg['response']}")
    return "|".join(msg_str)


def _rpc_is_request(msg: dict) -> bool:
    """ Returns True if the ``msg`` is a request RPC, False if it is a response """
    return msg['rpc'] > 0


class ClassEncoder(json.JSONEncoder):
    def default(self, o):
        if hasattr(o, '__reduce__') and o.__class__.__name__ in (Neighbour.__name__, RaftNeighbour.__name__):
            r = o.__reduce__()
            return {'__class__': r[0].__name__,
                    '__args__': r[1]}
        if isinstance(o, set):
            return list(o)
        return super().default(o)

    @staticmethod
    def object_hook(json_object):
        if json_object.get('__class__') == Neighbour.__name__:
            return Neighbour(*json_object['__args__'])
        if json_object.get('__class__') == RaftNeighbour.__name__:
            return RaftNeighbour(*json_object['__args__'])
        if json_object.get('__class__'):
            raise TypeError(f"Unrecognised class {json_object['__class__']}")
        return json_object


class BaseNode(object):
    """
    The base class for all cluster nodes.

    :param address: Address of the node
    """
    def __init__(self, address: str):
        self.address = address
        """ Address of the node. """

        self.meta = {}
        """ Meta data of the node. """

    def __str__(self):
        return self.address

    def __repr__(self):
        return f'{self.__class__.__name__}[address={self.address}]'


class Neighbour(BaseNode):
    """
    This is the base class for all cluster neighbours. It should be used to store neighbour specific information.
    """
    def __reduce__(self):
        return Neighbour, (self.address,)


class Node(BaseNode):
    """
    This class is the base class for all cluster node implementations. This class is used by Reactor in the case where
    no cluster is specified. The class assumes it is the leader immediately

    :param address: Address of the cluster node
    :param neighbours: List of addresses
    """
    def __init__(self, address: str, neighbours: list = None, neighbour_class=Neighbour):
        super().__init__(address)
        self.changed = time.time()
        self.leader = address
        self._neighbours = {n: neighbour_class(n) for n in neighbours or [] if n != address}
        self._neighbour_class = neighbour_class

    def is_leader(self) -> bool:
        """ Return whether the RaftNode is the leader of the cluster. """
        return True

    def has_leader(self) -> bool:
        """ Return whether the RaftNode's cluster has an elected leader (including itself). """
        return self.is_leader() or self.leader is not None

    @property
    def neighbours(self) -> Dict[str, Neighbour]:
        """ Returns the current dictionary of neighbours. """
        return self._neighbours

    @property
    def neighbourhood(self) -> list:
        """ List of addresses in the neighbourhood. """
        return [self.address] + list(self.neighbours.keys())

    def start(self) -> None:
        """ Start the cluster node. """
        pass

    def shutdown(self, timeout: int = None) -> None:
        """
        Shutdown the cluster node.

        :param timeout: Amount of time, in seconds, to block
        """
        pass

    def member(self, address: str) -> Optional[BaseNode]:
        """ Return the cluster member with ``address``. """
        return self if address == self.address else self._neighbours.get(address)


class ClusterException(Exception):
    """ Reactor raises ClusterExceptions when an issue occurs between cluster nodes. """
    pass


class RaftNeighbour(Neighbour):
    """
    This is a useful store of information about a neighbour node used by :py:class`RaftNode`.

    :param address: Address of the neighbour node, e.g. ``'node1:7000'``
    :param founder: True if neighbour is a founding cluster member
    :param membership: Status of the neighbours membership
    :param history_len: Length of history to keep
    """
    def __init__(self, address: str, founder: bool = True, membership: int = MEMBERSHIP_ACTIVE, history_len: int = 100):
        super().__init__(address)
        self.term = 0
        self.queued = History(history_len)
        self.sent = History(history_len)
        self.recv = History(history_len)
        self.ping = deque(maxlen=history_len)
        self.failed_count = 0
        self.contacted = False

        self._timestamp = time.time()
        self.founder = founder
        self.membership = membership
        self.msg = None
        self.neighbours = []

    def __reduce__(self):
        return RaftNeighbour, (self.address, self.founder, self.membership, self.ping.maxlen)

    def append_queued(self, msg: dict) -> None:
        """ Append a message to the queued history. """
        self.queued[msg['id']] = time.time()

    def append_sent(self, msg: dict) -> None:
        """ Append a message to the sent history and reset `failed_count` """
        self.sent[msg['id']] = time.time()
        self.failed_count = 0

    def append_recv(self, msg: dict) -> None:
        """ Append a message to the received history, update term, reset `failed_count`, and calculate ping. """
        if self.failed_count > 0:
            while self.queued.tail and self.awaiting_res():
                del self.queued[self.queued.tail]

        self.membership = MEMBERSHIP_ACTIVE if self.membership == MEMBERSHIP_ACTIVE else MEMBERSHIP_JOINING
        self.founder = False if self.membership == MEMBERSHIP_JOINING else self.founder
        self.recv[msg['id']] = time.time()
        self.term = msg['term']
        self.meta = msg['meta']
        self.neighbours = msg['neighbours']
        self.failed_count = 0
        if msg['id'] in self.sent:
            self.ping.append(self.recv[msg['id']] - self.sent[msg['id']])

    def awaiting_res(self) -> bool:
        """
        :return: Whether we are waiting for a response from the neighbour
        """
        return self.queued.tail not in self.recv

    @property
    def last_queued(self) -> float:
        return self.queued[self.queued.tail] if len(self.queued) else self._timestamp  # 0.0

    @property
    def last_sent(self) -> float:
        return self.sent[self.sent.tail] if len(self.sent) else self._timestamp  # 0.0

    @property
    def last_recv(self) -> float:
        return self.recv[self.recv.tail] if len(self.recv) else self._timestamp  # 0.0


class RaftNode(Node):
    """
    This is a simple implementation of the RAFT consensus algorithm to be used to determine the leadership.
    The implementation provides *no support* for maintaining a shared log history between the nodes.

    In this implementation there is no need for persistent state. If a node is dies it will lose the `term`
    it was in; on restart it will revert to `term = 1` and either will receive a heartbeat from the current
    leader with the actual current term or start an election and be rejected as its term is behind.

    As per section 5.6 of `the RAFT paper <https://raft.github.io/raft.pdf>`_:

        Raft will be able to elect and maintain a steady leader as long as the system satisfies the following
        *timing requirement*:

        ``broadcastTime << electionTimeout << minimumTimeBetweenFailure``


    :param address: Address of the cluster node, e.g. ``'node1:7000'``
    :param neighbours: List of addresses, e.g. ``['node2:7000', 'node3:7000']``
    :param election_timeout: Minimum time in seconds to wait for a leader before holding an election
    :param heartbeat_timeout: Minimum time in seconds for a leader to wait between sending heartbeats
    """
    MAX_TERMINATE_CALLED = 3
    """ Number of terminate calls before the threads are killed. """

    SOCKET_TIMEOUT = 0.5
    """ Timeout in seconds to blocking wait on socket before checking if terminated. """

    QUEUE_TIMEOUT = 0.5
    """ Timeout in seconds to blocking wait on queues before checking if terminated. """

    CIPHERS = 'EECDH+AESGCM:EDH+AESGCM:AES256+EECDH:AES256+EDH'
    """ Default ciphers to be used if `set_ssl` `ciphers` are `None`. """

    def __init__(self, address: str, neighbours: list,
                 election_timeout: float = ELECTION_TIMEOUT,
                 heartbeat_timeout: float = HEARTBEAT_TIMEOUT):
        super().__init__(address, neighbours, RaftNeighbour)

        self.leader = None

        self._terminate = threading.Event()
        self._term = 1
        self._founder = address in neighbours or not neighbours
        self._state = STATE_FOLLOWER if self._founder else STATE_ORPHAN

        self._queue = queue.Queue()
        self._elections = dict()
        self._timeout_time = 0
        self._election_timeout = election_timeout
        self._heartbeat_timeout = heartbeat_timeout

        self._failed_elections = 0
        self._ssl = None  # type: Optional[dict]

        self._threads = []
        self._execute_called = False
        self._terminate_called = 0

        # Check for read/write access
        if not os.access(os.curdir, os.R_OK | os.W_OK):
            raise ClusterException('Missing read/write access in current directory')

    @property
    def state(self) -> int:
        """ Return the state of the node. """
        return self._state

    @state.setter
    def state(self, state: int) -> None:
        """ Set the state of the node and make note of the time. """
        if state != self._state:
            self.changed = time.time()
        self._state = state

    def is_leader(self) -> bool:
        """ Return whether the RaftNode is the leader of the cluster. """
        return self.state == STATE_LEADER

    @property
    def neighbours(self) -> Dict[str, RaftNeighbour]:
        """ Returns a dictionary of active members of the cluster. """
        return {a: n for a, n in self._neighbours.items() if n.membership == MEMBERSHIP_ACTIVE}

    def is_majority(self, count: int, strong: bool = True) -> bool:
        """ Return whether the count is a majority of the neighbourhood. """
        return count >= self.majority if len(self.neighbours) % 2 == 0 or not strong else count > self.majority

    @property
    def majority(self) -> int:
        """ Number of nodes required to get a majority. """
        return math.ceil((len(self.neighbours)+1)/2)

    def set_ssl(self, key_file: str, crt_file: str, ca_crt: str, ciphers: str = None) -> None:
        """
        Set up SSL connection for transport communication.

        :param key_file: Path to the key file for the node
        :param crt_file: Path to the certificate file for the node
        :param ca_crt: Path to the CA file for the cluster
        :param ciphers: (optional) SSL ciphers to be used in RAFT node communication
        """
        self._ssl = {'key_file': key_file,
                     'crt_file': crt_file,
                     'ca_file': ca_crt,
                     'ciphers': ciphers or self.CIPHERS}

    def _wrap_socket(self, sock: socket.socket, server_hostname: str = None) -> socket.socket:
        """
        If SSL has been set, wrap the `sock` into an SSL context.

        :param sock: Socket to be wrapped
        :param server_hostname: Hostname of the server
        """
        if not self._ssl:
            return sock

        server_side = server_hostname is None

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH if server_side else ssl.Purpose.SERVER_AUTH)
        context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1 | ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_cert_chain(self._ssl['crt_file'], self._ssl['key_file'])
        context.load_verify_locations(self._ssl['ca_file'])
        if self._ssl['ciphers']:
            context.set_ciphers(self._ssl['ciphers'])

        return context.wrap_socket(sock,
                                   server_side=server_side,
                                   server_hostname=server_hostname)

    def _start_thread(self, *args, **kwargs):
        """
        Create a ``threading.Thread`` using ``*args`` and ``**kwargs``, append it to an internal list of threads, and
        then immediately start the thread.
        """
        thread = threading.Thread(*args, **kwargs)
        self._threads.append(thread)
        thread.start()

    def listen(self) -> None:
        """ Thread function to accept incoming connections from neighbouring RAFT nodes. """
        local_address, local_port = self.address.split(':', 2)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = self._wrap_socket(sock)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((local_address, int(local_port)))
        sock.settimeout(self.SOCKET_TIMEOUT)
        sock.listen(len(self._neighbours))

        logger = logging.getLogger('raft.listen')
        while not self._terminate.is_set():
            # Handle a connection from another RaftNode
            try:
                (client_sock, address) = sock.accept()

                self._start_thread(target=self.receive, args=(client_sock,), name='RAFT-receive', daemon=True)
            except socket.timeout:
                continue
            except ssl.SSLError as e:
                import traceback
                logger.critical(repr(e))
                logger.debug(traceback.print_exc())
                continue

        sock.close()

    def receive(self, client_sock: socket.socket) -> None:
        """
        Thread function to receive messages from a neighbouring RAFT node.

        :param client_sock: Socket to receive messages
        """
        client_sock.settimeout(self.SOCKET_TIMEOUT)
        logger = logging.getLogger('raft.recv')
        while not self._terminate.is_set():
            try:
                buffer = client_sock.recv(struct.calcsize(MSG_HEADER_FORMAT))
                msg_len = struct.unpack(MSG_HEADER_FORMAT, buffer)[0]
            except socket.timeout:
                time.sleep(0.001)
                continue
            except ConnectionError:
                time.sleep(0.001)
                continue
            except (ValueError, struct.error):
                time.sleep(0.001)
                continue

            # decode message
            msg = self.receive_msg(client_sock, msg_len)
            if msg is None:
                continue

            logger.debug('Recv %s', _rpc_str(msg))

            if msg['sender'] not in self._neighbours:
                self._neighbours[msg['sender']] = RaftNeighbour(msg['sender'], False, MEMBERSHIP_JOINING)
                logger.info('Added neighbour: %s', msg['sender'])
            self._neighbours[msg['sender']].append_recv(msg)
            # handle message
            if msg['term'] > self._term:
                self.state = STATE_FOLLOWER if self.state != STATE_ORPHAN else STATE_ORPHAN
                self._term = msg['term']

            # Handle RPC requests
            if msg['rpc'] == RPC_PING:
                self._handle_rpc_ping(msg, logger)
            if msg['rpc'] == RPC_APPEND_ENTRIES:
                self._handle_rpc_append_entries(msg, logger)
            elif msg['rpc'] == RPC_VOTE_ELECTION:
                self._handle_rpc_vote_request(msg, logger)
            elif msg['rpc'] == RPC_CLUSTER_JOIN:
                self._handle_rpc_cluster_join_request(msg, logger)
            elif msg['rpc'] == RPC_CLUSTER_LEAVE:
                self._handle_rpc_cluster_leave_request(msg, logger)

            # Handle RPC responses
            elif msg['rpc'] == -RPC_PING:
                self._handle_rpc_ping(msg, logger)
            elif msg['rpc'] == -RPC_APPEND_ENTRIES:
                self._handle_rpc_append_entries(msg, logger)
            elif msg['rpc'] == -RPC_VOTE_ELECTION:
                self._handle_rpc_vote_response(msg, logger)
            elif msg['rpc'] == -RPC_CLUSTER_JOIN:
                self._handle_rpc_cluster_join_response(msg, logger)
            elif msg['rpc'] == -RPC_CLUSTER_LEAVE:
                self._handle_rpc_cluster_leave_response(msg, logger)

            time.sleep(0.001)

        client_sock.close()

    def receive_msg(self, sock: socket.socket, msg_len: int) -> Optional[dict]:
        """
        Attempt to read a message from `sock` that `msg_len` bytes long.

        :param sock: Socket to read the buffer
        :param msg_len: Length of the message
        :return: Parsed message or None
        """
        buffer = b''
        while not self._terminate.is_set():
            try:
                while len(buffer) < msg_len:
                    buffer += sock.recv(min(255, msg_len-len(buffer)))
                return json.loads(buffer, object_hook=ClassEncoder.object_hook)
            except socket.timeout:
                pass
        return None

    def queue_msg(self, msg: dict) -> None:
        """ Queue `msg` to be sent and add to the recipient neighbours' queued history. """
        neighbour = self._neighbours[msg['recipient']]
        neighbour.append_queued(msg)
        self._queue.put(msg)

    def send(self) -> None:
        """ Thread function to poll the message queue and send the message to specified neighbour. """
        pool = dict()
        msg = recipient = None
        logger = logging.getLogger('raft.send')
        while not self._terminate.is_set():
            try:
                msg = self._queue.get(block=True, timeout=self.QUEUE_TIMEOUT)
                recipient = msg['recipient']
                msg['term'] = self._term
                msg['sender'] = self.address
                msg['meta'] = self.meta
                msg['state'] = self.state
                msg['neighbours'] = self._neighbours

                if recipient not in pool:
                    sock = socket.create_connection(recipient.split(':', 2))
                    sock = self._wrap_socket(sock, recipient.split(':', 2)[0])
                    pool[recipient] = sock

                msg_body = json.dumps(msg, cls=ClassEncoder).encode('utf-8')
                msg_header = struct.pack(MSG_HEADER_FORMAT, len(msg_body))
                buffer = msg_header + msg_body
                pool[recipient].sendall(buffer)
                logger.debug('Sent %s', _rpc_str(msg))

                if recipient not in self._neighbours:
                    self._neighbours[recipient] = RaftNeighbour(recipient, False, MEMBERSHIP_JOINING)
                self._neighbours[recipient].append_sent(msg)
                msg = recipient = None

            except queue.Empty:
                continue
            except ConnectionError:
                # Increment the failed send count
                self._handle_connection_failure(msg, logger)
                # Tidy up the connection pool
                if recipient is not None and recipient in pool:
                    pool[recipient].close()
                    del pool[recipient]
            except Exception as e:
                import traceback
                logger.error(repr(e))
                logger.debug(traceback.print_exc())
                # Increment the failed send count
                self._handle_connection_failure(msg, logger)
                continue

            if self._queue.empty():
                time.sleep(0.001)

        for n in pool:
            pool[n].close()

    def execute(self) -> None:
        """ A blocking function that will execute the RAFT node. """
        if self._execute_called:
            raise RuntimeError('RaftNode already executing')

        # Load neighbours from persistent storage
        self._load_neighbours()

        logger = logging.getLogger('raft.exec')
        self._execute_called = True
        self._terminate.clear()
        logger.info('Starting up %s', self.address)
        self._timeout_time = _next_timeout(STATE_FOLLOWER, 0, self._election_timeout, self._heartbeat_timeout)
        self._start_thread(target=self.send, name='RAFT-send')
        self._start_thread(target=self.listen, name='RAFT-listen')

        # If there are no neighbours, immediately elect yourself
        if not self.neighbours:
            self._handle_inauguration(logger)

        while not self._terminate.is_set():
            # Orphan
            if self.state == STATE_ORPHAN:
                # If we have timeout out
                if time.time() > self._timeout_time and self.neighbours:
                    neighbour = random.choice(list(self.neighbours.keys()))
                    logger.debug('Requesting cluster leader from %s', neighbour)
                    # Send a random neighbour a ping request and wait for up to heartbeat timeout
                    self.queue_msg(_rpc_request(RPC_PING, neighbour))
                    self._timeout_time = time.time() + self._heartbeat_timeout
            # Follower
            elif self.state == STATE_FOLLOWER:
                # If election timeout reached
                if time.time() > self._timeout_time:
                    logger.warning('Convert to candidate')
                    self.state = STATE_CANDIDATE
                    self.leader = None
                    continue
            # Candidate
            elif self.state == STATE_CANDIDATE:
                if time.time() > self._timeout_time:
                    self._failed_elections += 1
                    logger.warning('Election timeout %s|%s', self._term, self._failed_elections)
                    self._term += 1
                    self._elections[self._term] = {self.address: self.address}
                    self._timeout_time = _next_timeout(self.state, self._failed_elections,
                                                       self._election_timeout, self._heartbeat_timeout)
                    for neighbour in self.neighbours.values():
                        self.queue_msg(_rpc_request(RPC_VOTE_ELECTION, neighbour.address))
                    continue
                # If we have a majority
                if self.is_majority(sum(1 for _ in filter(None, self._elections[self._term].values()))):
                    self._handle_inauguration(logger)
            # Leader
            elif self.state == STATE_LEADER:
                # Send heartbeats
                for neighbour in self.neighbours.values():
                    heartbeat_timeout = neighbour.last_sent + (self._heartbeat_timeout * 2 ** neighbour.failed_count)
                    if not neighbour.contacted or (time.time() > heartbeat_timeout and not neighbour.awaiting_res()):
                        neighbour.contacted = True
                        self.queue_msg(_rpc_request(RPC_APPEND_ENTRIES, neighbour.address))
                    elif neighbour.contacted and time.time() > heartbeat_timeout:
                        neighbour.failed_count += 1

                # Output ping information
                if time.time() > self._timeout_time:
                    self._timeout_time = time.time() + 5.0
                    pings = []
                    cnt_ping = 0
                    tot_ping = 0
                    for neighbour in self.neighbours.values():
                        if len(neighbour.ping) and neighbour.failed_count == 0:
                            tot_ping += neighbour.sent.total
                            cnt_ping += len(neighbour.ping)
                            pings.extend(neighbour.ping)
                    if len(pings) > 1:
                        mean_ping = statistics.mean(pings) * 1000.0
                        stdev_ping = statistics.stdev(pings, mean_ping) * 1000.0
                        statistics.median_grouped(pings)
                        logger.debug('Average ping %3d/%3d: %5.1fms [%.5f]', cnt_ping, tot_ping, mean_ping, stdev_ping)
                    continue

                # If we haven't heard from a majority of our neighbours within `election_timeout` seconds
                not_responding_count = 0
                for neighbour in self.neighbours.values():
                    t = max(self.changed, neighbour.last_queued, neighbour.last_sent, neighbour.last_recv)
                    if (time.time() - t) > self._election_timeout:
                        not_responding_count += 1
                if self.is_majority(not_responding_count, False):
                    # Step down as a leader
                    logger.warning('Stepping down as leader: majority lost (%d)', not_responding_count)
                    self.state = STATE_FOLLOWER
                    self._timeout_time = _next_timeout(STATE_FOLLOWER, 0,
                                                       self._election_timeout, self._heartbeat_timeout)
            else:
                # Unknown state
                pass

            # If there isn't a leader or we are the leader
            if not self.has_leader() or self.is_leader():
                # If we haven't heard from a non founder neighbour within 4 times `election_timeout` seconds
                for neighbour in self.neighbours.values():
                    if not neighbour.founder and neighbour.last_recv + (4 * self._election_timeout) < time.time():
                        logger.info('Evicting %s from cluster', neighbour.address)
                        neighbour.membership = MEMBERSHIP_LEAVING
                        self._store_neighbours(logger)

            time.sleep(0.001)

        self._execute_called = False

    def start(self) -> None:
        """ Start RAFT execution in a thread. """
        self._start_thread(target=self.execute, name='RAFT-execute')

    def shutdown(self, timeout: int = None) -> None:
        """ Set the terminate flag and increment """
        logger = logging.getLogger('raft')
        self._terminate_called += 1
        if self._terminate_called >= self.MAX_TERMINATE_CALLED:
            logger.error(f"RaftNode failed to terminate after {self._terminate_called} attempts")
            raise ClusterException()

        # Attempt to leave the cluster
        if not self._founder:
            logger.info('Attempting to leave cluster')
            for neighbour in self._neighbours.values():
                if neighbour.membership != MEMBERSHIP_ACTIVE:
                    continue
                neighbour.membership = MEMBERSHIP_LEAVING
                self.queue_msg(_rpc_request(RPC_CLUSTER_LEAVE, neighbour.address))
                logger.info('Sent leave request to %s', neighbour.address)
            self.state = STATE_ORPHAN
            # Wait for at least one node to receive the message
            self._terminate.wait(timeout or self._election_timeout)

        self._terminate.set()
        logger.info('Attempting normal shutdown')

        for thread in self._threads:
            thread.join(timeout)

        if not self._founder:
            self._remove_neighbours(logger)

    def _hash_neighbours(self):
        return '|'.join([f'{n.address}:{n.membership}' for n in self.neighbours.values()])

    def _remove_neighbours(self, logger: logging.Logger):
        if os.path.exists(f'{self.address}.json') and os.path.isfile(f'{self.address}.json'):
            os.remove(f'{self.address}.json')
            logger.debug('Removed persistent storage')

    def _store_neighbours(self, logger: logging.Logger):
        """ Store the neighbours in our """
        with open(f'{self.address}.json', 'w') as fh:
            neighbours = [{'address': self.address, 'founder': self._founder}]
            neighbours += [{'address': n.address, 'founder': n.founder} for n in self.neighbours.values()]
            json.dump(neighbours, fh, cls=ClassEncoder)
            logger.debug('Updating persistent storage')

    def _load_neighbours(self):
        try:
            with open(f'{self.address}.json', 'r') as fh:
                neighbours = json.load(fh, object_hook=ClassEncoder.object_hook)
                self._state = STATE_FOLLOWER
                for n in neighbours:
                    if n['address'] == self.address:
                        continue
                    self._neighbours.setdefault(n['address'], RaftNeighbour(n['address'], False, MEMBERSHIP_ACTIVE))

        except FileNotFoundError:
            pass

    def _handle_connection_failure(self, msg: dict, logger: logging.Logger) -> None:
        """ Handles connection failure to ``msg`` recipient. """
        recipient = msg['recipient']
        if msg is not None:
            if self._neighbours[recipient].failed_count == 0:
                logger.warning('Connection error %s', recipient)
            else:
                logger.info('Connection error %s (%s)', recipient, self._neighbours[recipient].failed_count)

            self._neighbours[recipient].failed_count += 1
            del self._neighbours[recipient].queued[msg['id']]

    def _handle_rpc_ping(self, msg, logger: logging.Logger):
        # If msg is a RPC_PING request
        if _rpc_is_request(msg):
            self.queue_msg(_rpc_response(msg, self.leader))
        # Otherwise, msg is a response
        # If we are not an orphan
        elif self.state != STATE_ORPHAN:
            return
        # If msg has a response
        elif msg['response']:
            if msg['response'] not in self._neighbours:
                self._neighbours[msg['response']] = msg['neighbours'][msg['response']]
            # Send RPC_CLUSTER_JOIN request to leader
            self.queue_msg(_rpc_request(RPC_CLUSTER_JOIN, msg['response']))
            # Extend timeout to wait for an election timeout
            self._timeout_time = time.time() + self._election_timeout
            logger.debug('Attempting to join cluster (leader=%s)', msg['response'])
        # If msg is a response has no leader extend to allow time for an election
        else:
            # Extend timeout to wait for an election timeout
            self._timeout_time = time.time() + (2 * self._election_timeout)
            logger.debug('Waiting for cluster to elected leader')

    def _handle_rpc_append_entries(self, msg, logger: logging.Logger):
        """ Handles RPC ``APPEND_ENTRIES``. """
        if _rpc_is_request(msg):
            response = msg['term'] >= self._term
            self.queue_msg(_rpc_response(msg, response))
            if response and self.leader != msg['sender']:
                self.leader = msg['sender']
                self._neighbours[msg['sender']].membership = MEMBERSHIP_ACTIVE
                logger.warning('Accepted leader %s | %s', self._term, msg['sender'])

            self._failed_elections = 0

            # Update neighbours
            neighbours_hash = self._hash_neighbours()
            for neighbour in msg['neighbours']:
                # Skip ourself
                if neighbour == self.address:
                    continue
                # Add new neighbours
                elif neighbour not in self._neighbours:
                    self._neighbours[neighbour] = msg['neighbours'][neighbour]
                self._neighbours[neighbour].membership = msg['neighbours'][neighbour].membership
            # Remove neighbour
            list(map(self._neighbours.pop, set(self._neighbours.keys() - ({msg['sender']} | msg['neighbours'].keys()))))
            if neighbours_hash != self._hash_neighbours():
                # Store neighbours into persistent storage if there was a change
                self._store_neighbours(logger)

            self.leader = msg['sender']
            self.state = STATE_FOLLOWER if self._founder or self.address in self.member(self.leader).neighbours else STATE_ORPHAN
            self._term = msg['term']
            self._timeout_time = _next_timeout(self.state, 0, self._election_timeout, self._heartbeat_timeout)
        else:
            # If there is a majority of neighbours that have accepted the pending neighbour
            for pending_neighbour in [n for n in self._neighbours.values() if n.membership == MEMBERSHIP_JOINING]:
                count = sum([1 for n in self.neighbours.values() if pending_neighbour.address in n.neighbours])
                if self.is_majority(1 + count) and isinstance(pending_neighbour.msg, dict):
                    pending_neighbour.membership = MEMBERSHIP_ACTIVE
                    self.queue_msg(_rpc_response(pending_neighbour.msg, True))
                    logger.info('Node joined cluster %s', pending_neighbour.address)
                    pending_neighbour.msg = None
                    self._store_neighbours(logger)

    def _handle_rpc_vote_request(self, msg, logger: logging.Logger):
        """ Handles RPC ``VOTE_REQUEST``. """
        # If the vote is from an old term or we are a candidate
        if msg['term'] < self._term or self.state in {STATE_CANDIDATE, STATE_LEADER}:
            response = False
            self.queue_msg(_rpc_response(msg, response))

        # If we have voted in this term
        elif self._term in self._elections and self.address in self._elections[self._term]:
            response = self._elections[self._term][self.address] == msg['sender']
            self.queue_msg(_rpc_response(msg, response))
            self._timeout_time = _next_timeout(self.state, 0, self._election_timeout, self._heartbeat_timeout)

        else:
            response = True
            self._elections[self._term] = {self.address: msg['sender']}
            self.queue_msg(_rpc_response(msg, response))
            self._timeout_time = _next_timeout(self.state, 0, self._election_timeout, self._heartbeat_timeout)

        logger.debug('Voted for %s in term %d: %r', msg['sender'], msg['term'], response)

    def _handle_rpc_vote_response(self, msg, logger: logging.Logger):
        """ Handles RPC ``VOTE_RESPONSE``. """
        if msg['term'] not in self._elections:
            self._elections[msg['term']] = dict()
        self._elections[msg['term']][msg['sender']] = msg['response']

        if msg['state'] == STATE_LEADER:
            self._timeout_time = _next_timeout(STATE_FOLLOWER, 0, self._election_timeout, self._heartbeat_timeout)

        if self.state != STATE_CANDIDATE:
            return

        # If we have a majority
        if self.is_majority(sum(1 for _ in filter(None, self._elections[self._term].values()))):
            self._handle_inauguration(logger)

    def _handle_rpc_cluster_join_request(self, msg, logger: logging.Logger):
        # If we are not the leader - reject the request
        if not self.is_leader():
            self.queue_msg(_rpc_response(msg, False))
        # If we are the only member of the cluster - immediately add sender
        elif len(self.neighbours) == 0:
            self._neighbours[msg['sender']].membership = MEMBERSHIP_ACTIVE
            self.queue_msg(_rpc_response(msg, True))
            logger.info('Node joined cluster %s', msg['sender'])
            self._store_neighbours(logger)
        # If there are multiple members add the sender to the pending members set
        else:
            self._neighbours[msg['sender']].membership = MEMBERSHIP_JOINING
            self._neighbours[msg['sender']].founder = False
            self._neighbours[msg['sender']].msg = msg

    def _handle_rpc_cluster_join_response(self, msg, logger: logging.Logger):
        if not msg['response']:
            # Extend timeout to wait for an election timeout
            self._timeout_time = time.time() + (2 * self._election_timeout)
        else:
            for neighbour in msg['neighbours']:
                if neighbour != self.address:
                    self._neighbours.setdefault(neighbour, RaftNeighbour(neighbour, False, MEMBERSHIP_ACTIVE))
            self._store_neighbours(logger)
            logger.info('Accepted into cluster')
            self._timeout_time = time.time() + (2 * self._election_timeout)
            self.state = STATE_FOLLOWER

    def _handle_rpc_cluster_leave_request(self, msg, logger: logging.Logger):
        # If the sender is a member of the cluster
        if msg['sender'] in self.neighbours:
            # Mark the neighbour as leaving the cluster
            self._neighbours[msg['sender']].membership = MEMBERSHIP_LEAVING
            logger.info('Node leaving cluster %s', msg['sender'])
            self._store_neighbours(logger)

        self.queue_msg(_rpc_response(msg, True))

    def _handle_rpc_cluster_leave_response(self, msg, logger: logging.Logger):
        # If the sender accepted our leave request
        if msg['response']:
            self._terminate.set()
            logger.info('%s accepted leave request', msg['sender'])

    def _handle_inauguration(self, logger: logging.Logger) -> None:
        """ Handles node inauguration. """
        self.state = STATE_LEADER
        self._timeout_time = time.time()
        self._failed_elections = 0
        self.leader = self.address
        for neighbour in self.neighbours.values():
            neighbour.contacted = False
        logger.warning('Elected leader %s', self._term)


class History(OrderedDict):
    """
    An ordered dictionary with a maximum size.

    :param maxsize: Maximum size of the ordered dictionary
    """
    def __init__(self, maxsize: int = 128, *args, **kwargs):
        self.maxsize = maxsize
        self._total = 0
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self._total += 1
        if len(self) > self.maxsize:
            self.popitem(False)

    def __delitem__(self, key) -> None:
        if len(self) <= self.maxsize:
            self._total -= 1
        super().__delitem__(key)

    def clear(self) -> None:
        super().clear()
        self._total = 0

    @property
    def head(self) -> Optional[any]:
        """ The least recently added key. """
        return next(iter(self.keys())) if len(self) else None

    @property
    def tail(self) -> Optional[any]:
        """ The most recently added key. """
        return next(iter(reversed(self.keys()))) if len(self) else None

    @property
    def total(self) -> int:
        """ The total number of items added to the history (including deleted and removed). """
        return self._total


if __name__ == '__main__':
    import sys
    import signal

    raft_logger = logging.getLogger('raft')
    ch = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-8s - %(message)s')
    ch.setFormatter(formatter)
    raft_logger.addHandler(ch)
    raft_logger.setLevel(logging.INFO)

    port = int(sys.argv[1])
    raft_node = RaftNode(f'localhost:{int(port)}', [f'localhost:{int(p)}' for p in sys.argv[2:]], 0.5, 0.1)
    raft_node.set_ssl('./certs/transport-consensus.key',
                      './certs/transport-consensus.crt',
                      './certs/transport-ca.pem')

    def handle_sigint(signal_num, _):
        if signal_num == signal.SIGINT:
            raft_node.shutdown()

    signal.signal(signal.SIGINT, handle_sigint)
    raft_thread = threading.Thread(target=raft_node.execute, name='RAFT-execute')
    raft_thread.start()
    raft_thread.join()
