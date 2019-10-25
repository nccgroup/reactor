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
import logging
import math
import pickle
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

from reactor.exceptions import ClusterException

# By default add the null handler to the library logger
logging.getLogger('raft').addHandler(logging.NullHandler())

STATE_FOLLOWER = 1
STATE_CANDIDATE = 2
STATE_LEADER = 3

ELECTION_TIMEOUT = 0.5
HEARTBEAT_TIMEOUT = 0.1

RPC_APPEND_ENTRIES = 1
RPC_APPEND_RESPONSE = 2
RPC_VOTE_REQUEST = 3
RPC_VOTE_RESPONSE = 4

MSG_HEADER_FORMAT = '>H'


_rpcToName = {
    RPC_APPEND_ENTRIES:  'append req',
    RPC_APPEND_RESPONSE: 'append res',
    RPC_VOTE_REQUEST:    'vote   req',
    RPC_VOTE_RESPONSE:   'vote   res',
}


def _next_timeout(raft_state: int, exponent: int, election_timeout: float, heartbeat_timeout: float) -> float:
    """ Calculates the next timeout time based on the `raft_state` and timeout times. """
    if raft_state == STATE_FOLLOWER:
        return time.time() + random.uniform(election_timeout, 2 * election_timeout)
    elif raft_state == STATE_CANDIDATE:
        election_timeout *= 2 ** exponent
        return time.time() + random.uniform(election_timeout, 2 * election_timeout)
    elif raft_state == STATE_LEADER:
        heartbeat_timeout *= 2 ** exponent
        return time.time() + heartbeat_timeout


def _rpc_request(rpc: int, term: int, recipient) -> dict:
    """
    Creates a request RPC.
    :param rpc: Procedure being requested
    :param term: Current term of the requester
    :param recipient: Recipient of the message
    """
    return {'id': random.getrandbits(16),
            'term': term,
            'rpc': rpc,
            'recipient': recipient}


def _rpc_response(msg: dict, term: int, response: bool) -> dict:
    """
    Creates a response RPC.
    :param msg: RPC request message
    :param term: Current term of the responder
    :param response: Response to the request
    """
    if msg['rpc'] == RPC_APPEND_ENTRIES:
        rpc = RPC_APPEND_RESPONSE
    else:
        rpc = RPC_VOTE_RESPONSE
    return {'id': msg['id'],
            'term': term,
            'rpc': rpc,
            'recipient': msg['sender'],
            'response': response}


def _rpc_str(msg: dict) -> str:
    """ Generate a string of the RPC `msg`. """
    msg_str = f"#{msg['id']:>5}|{msg['term']:>2}|{_rpcToName[msg['rpc']]}|{msg['recipient']}|{msg['sender']}"
    if msg['rpc'] in (RPC_APPEND_RESPONSE, RPC_VOTE_RESPONSE):
        msg_str += f"|{msg['response']}"
    return msg_str


class Neighbour(object):
    """
    This is the base class for all cluster neighbours. It should be used to store neighbour specific information that
    such as ``id``, ``address``, or ``meta`` data.

    :param address: Address of the neighbour node
    """
    def __init__(self, address: str):
        self.address = address
        self.meta = {}


class Node(object):
    """
    This class is the base class for all cluster node implementations. This class is used by Reactor in the case where
    no cluster is specified. The class assumes it is the leader immediately

    :param address: Address of the cluster node
    :param neighbours: List of addresses
    """
    def __init__(self, address: str, neighbours: list = None, neighbour_class=Neighbour):
        self.changed = time.time()
        self.leader = address
        self.address = address
        self._neighbours = {n: neighbour_class(n) for n in neighbours or [] if n != address}

        self._meta = {}

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

    def set_ssl(self, *args, **kwargs):
        pass

    def start(self) -> None:
        """ Start the cluster node. """
        pass

    def shutdown(self, timeout: int = None) -> None:
        """
        Shutdown the cluster node.

        :param timeout: Amount of time, in seconds, to block
        """
        pass

    @property
    def meta(self) -> dict:
        """
        Returns the meta data of the node. Meta data is sent with every message to neighbouring nodes and stored.
        """
        return self._meta

    def leader_meta(self) -> dict:
        """ Returns the meta data of the cluster leader. """
        if self.leader and not self.is_leader():
            return self.neighbours[self.leader].meta
        elif self.is_leader():
            return self._meta
        else:
            return {}


class RaftNeighbour(Neighbour):
    """
    This is a useful store of information about a neighbour node used by :py:class`RaftNode`.

    :param address: Address of the neighbour node, e.g. ``'node1:7000'``
    :param history_len: Length of history to keep
    """
    def __init__(self, address: str, history_len: int = 100):
        super().__init__(address)
        self.term = 0
        self.queued = History(history_len)
        self.sent = History(history_len)
        self.recv = History(history_len)
        self.ping = deque(maxlen=history_len)
        self.failed_count = 0
        self.contacted = False

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

        self.recv[msg['id']] = time.time()
        self.term = msg['term']
        self.meta = msg['meta']
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
        return self.queued[self.queued.tail] if len(self.queued) else 0.0

    @property
    def last_sent(self) -> float:
        return self.sent[self.sent.tail] if len(self.sent) else 0.0

    @property
    def last_recv(self) -> float:
        return self.recv[self.recv.tail] if len(self.recv) else 0.0


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
        self._state = STATE_FOLLOWER

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
        return self._neighbours

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
        sock.listen(len(self.neighbours))

        logger = logging.getLogger('raft.listen')
        while not self._terminate.is_set() and len(self.neighbours):
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

            self.neighbours[msg['sender']].append_recv(msg)
            # handle message
            if msg['term'] > self._term:
                self.state = STATE_FOLLOWER
                self._term = msg['term']

            if msg['rpc'] == RPC_APPEND_ENTRIES:
                self._handle_rpc_append_entries(msg, logger)
            elif msg['rpc'] == RPC_APPEND_RESPONSE:
                pass
            elif msg['rpc'] == RPC_VOTE_REQUEST:
                self._handle_rpc_vote_request(msg, logger)
            elif msg['rpc'] == RPC_VOTE_RESPONSE:
                self._handle_rpc_vote_response(msg, logger)

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
                return pickle.loads(buffer)
            except socket.timeout:
                pass
        return None

    def queue_msg(self, msg: dict) -> None:
        """ Queue `msg` to be sent and add to the recipient neighbours' queued history. """
        neighbour = self.neighbours[msg['recipient']]
        neighbour.append_queued(msg)
        self._queue.put(msg)

    def send(self) -> None:
        """ Thread function to poll the message queue and send the message to specified neighbour. """
        pool = dict()
        msg = recipient = None
        logger = logging.getLogger('raft.send')
        while not self._terminate.is_set() and len(self.neighbours):
            try:
                msg = self._queue.get(block=True, timeout=self.QUEUE_TIMEOUT)
                recipient = msg['recipient']
                msg['term'] = self._term
                msg['sender'] = self.address
                msg['meta'] = self.meta
                msg['state'] = self.state

                if recipient not in pool:
                    sock = socket.create_connection(recipient.split(':', 2))
                    sock = self._wrap_socket(sock, recipient.split(':', 2)[0])
                    pool[recipient] = sock

                msg_body = pickle.dumps(msg)
                msg_header = struct.pack(MSG_HEADER_FORMAT, len(msg_body))
                buffer = msg_header + msg_body
                pool[recipient].sendall(buffer)
                logger.debug('Sent %s', _rpc_str(msg))

                self.neighbours[recipient].append_sent(msg)
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

        logger = logging.getLogger('raft.exec')
        self._execute_called = True
        self._terminate.clear()
        logger.info('Starting up %s', self.address)
        self._timeout_time = _next_timeout(STATE_FOLLOWER, 0, self._election_timeout, self._heartbeat_timeout)
        self._start_thread(target=self.send, name='RAFT-send')
        self._start_thread(target=self.listen, name='RAFT-listen')

        # If there are no neighbours, immediately elect yourself and sleep until shutdown is called
        if not self.neighbours:
            self._handle_inauguration(logger)
            self._terminate.wait()

        while not self._terminate.is_set():
            # Follower
            if self.state == STATE_FOLLOWER:
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
                        msg = _rpc_request(RPC_VOTE_REQUEST, self._term, neighbour.address)
                        self.queue_msg(msg)
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
                        msg = _rpc_request(RPC_APPEND_ENTRIES, self._term, neighbour.address)
                        self.queue_msg(msg)
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

            time.sleep(0.001)

        self._execute_called = False

    def start(self) -> None:
        """ Start RAFT execution in a thread. """
        self._start_thread(target=self.execute, name='RAFT-execute')

    def shutdown(self, timeout: int = None) -> None:
        """ Set the terminate flag and increment """
        logger = logging.getLogger('raft')
        self._terminate_called += 1
        self._terminate.set()
        if self._terminate_called == self.MAX_TERMINATE_CALLED:
            logger.error(f"RaftNode failed to terminate after {self._terminate_called} attempts")
            raise ClusterException()
        else:
            logger.info('Attempting normal shutdown')

        for thread in self._threads:
            thread.join(timeout)

    def _handle_connection_failure(self, msg: dict, logger: logging.Logger) -> None:
        """ Handles connection failure to ``msg`` recipient. """
        recipient = msg['recipient']
        if msg is not None:
            if self.neighbours[recipient].failed_count == 0:
                logger.warning('Connection error %s', recipient)
            else:
                logger.debug('Connection error %s (%s)', recipient, self.neighbours[recipient].failed_count)

            self.neighbours[recipient].failed_count += 1
            del self.neighbours[recipient].queued[msg['id']]

    def _handle_rpc_append_entries(self, msg, logger: logging.Logger):
        """ Handles RPC ``APPEND_ENTRIES``. """
        self.state = STATE_FOLLOWER
        self._term = msg['term']
        self._timeout_time = _next_timeout(self.state, 0, self._election_timeout, self._heartbeat_timeout)
        response = msg['term'] >= self._term
        self.queue_msg(_rpc_response(msg, self._term, response))
        if response and self.leader != msg['sender']:
            self.leader = msg['sender']
            logger.warning('Accepted leader %s | %s', self._term, msg['sender'])
        self._failed_elections = 0

    def _handle_rpc_vote_request(self, msg, logger: logging.Logger):
        """ Handles RPC ``VOTE_REQUEST``. """
        # If the vote is from an old term or we are a candidate
        if msg['term'] < self._term or self.state in {STATE_CANDIDATE, STATE_LEADER}:
            response = False
            self.queue_msg(_rpc_response(msg, self._term, response))

        # If we have voted in this term
        elif self._term in self._elections and self.address in self._elections[self._term]:
            response = self._elections[self._term][self.address] == msg['sender']
            self.queue_msg(_rpc_response(msg, self._term, response))
            self._timeout_time = _next_timeout(self.state, 0, self._election_timeout, self._heartbeat_timeout)

        else:
            response = True
            self._elections[self._term] = {self.address: msg['sender']}
            self.queue_msg(_rpc_response(msg, self._term, response))
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
    """ An ordered dictionary with a maximum size. """
    def __init__(self, maxsize: int = 128, *args, **kwargs):
        """
        :param maxsize: Maximum size of the ordered dictionary
        """
        self.maxsize = maxsize
        self._total = 0
        self._tail = None
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self._total += 1
        self._tail = key
        if len(self) > self.maxsize:
            self.popitem(False)

    def __delitem__(self, key) -> None:
        super().__delitem__(key)
        self._total -= 1
        self._tail = deque(self, maxlen=1).pop() if len(self) else None

    @property
    def head(self):
        """ The least recently added key. """
        return next(iter(self))

    @property
    def tail(self):
        """ The most recently added key. """
        return self._tail

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
