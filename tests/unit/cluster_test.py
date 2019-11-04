import pytest
import time
from freezegun import freeze_time

from reactor.cluster import STATE_ORPHAN, STATE_FOLLOWER, STATE_CANDIDATE, STATE_LEADER
from reactor.cluster import _next_timeout, _rpc_request, _rpc_response, _rpc_is_request
from reactor.cluster import ClassEncoder, Neighbour, Node, History


@freeze_time("2000-01-01 12:00:00")
def test_next_timeout_state_orphan():
    now = time.time()
    e_timeout = 10.0
    h_timeout = 2.0

    for exp in range(4):
        assert now + e_timeout <= _next_timeout(STATE_ORPHAN, exp, e_timeout, h_timeout) <= now + (2 * e_timeout)


@freeze_time("2000-01-01 12:00:00")
def test_next_timeout_state_follower():
    now = time.time()
    e_timeout = 10.0
    h_timeout = 2.0

    for exp in range(4):
        assert now + e_timeout <= _next_timeout(STATE_FOLLOWER, exp, e_timeout, h_timeout) <= now + (2 * e_timeout)


@freeze_time("2000-01-01 12:00:00")
def test_next_timeout_state_candidate():
    now = time.time()
    e_timeout = 10.0
    h_timeout = 2.0

    for exp in range(4):
        exp_e_timeout = 2**exp * e_timeout
        assert now + exp_e_timeout <= _next_timeout(STATE_CANDIDATE, exp, e_timeout, h_timeout) <= now + (2 * exp_e_timeout)


@freeze_time("2000-01-01 12:00:00")
def test_next_timeout_state_leader():
    now = time.time()
    e_timeout = 10.0
    h_timeout = 2.0

    for exp in range(4):
        exp_h_timeout = 2**exp * h_timeout
        assert _next_timeout(STATE_LEADER, exp, e_timeout, h_timeout) == now + exp_h_timeout


def test_rpc_request_response():
    for state in (STATE_ORPHAN, STATE_FOLLOWER, STATE_CANDIDATE, STATE_LEADER):
        msg = _rpc_request(state, 'recipient:7000')
        assert set(msg.keys()) == {'id', 'term', 'rpc', 'recipient'}
        assert type(msg['id']) == int and 0 <= msg['id'] < 2**16
        assert msg['rpc'] == state
        assert msg['recipient'] == 'recipient:7000'
        assert _rpc_is_request(msg)
        msg['sender'] = 'sender:7000'

        res = _rpc_response(msg, 'response_value')
        assert set(res.keys()) == {'id', 'term', 'rpc', 'recipient', 'response'}
        assert res['id'] == msg['id']
        assert res['rpc'] == -state
        assert res['recipient'] == 'sender:7000'
        assert not _rpc_is_request(res)


def test_class_encoder():
    encoder = ClassEncoder()
    with pytest.raises(TypeError):
        encoder.default(1)
    with pytest.raises(TypeError):
        encoder.default('')
    with pytest.raises(TypeError):
        encoder.default(True)
    with pytest.raises(TypeError):
        encoder.default(object())

    encoded = encoder.default(Neighbour('neighbour:7000'))
    assert encoded.get('__class__') == Neighbour.__name__
    assert encoded.get('__args__') == ('neighbour:7000', )

    decoded = encoder.object_hook(encoded)
    assert isinstance(decoded, Neighbour)
    assert decoded.address == 'neighbour:7000'


# Tests for ``History`` class
def test_history():
    history = History(3)
    assert history.maxsize == 3
    assert history.head is None
    assert history.tail is None
    assert history.total == 0

    # Test adding within max size
    history['foo'] = 'bar'
    assert history.total == len(history) == 1
    assert history.head == history.tail == 'foo'

    history['bar'] = 'baz'
    assert history.total == len(history) == 2
    assert history.head == 'foo'
    assert history.tail == 'bar'

    history['baz'] = 'qux'
    assert history.total == len(history) == 3
    assert history.head == 'foo'
    assert history.tail == 'baz'

    # Test adding when max size is reached
    history['qux'] = 'foo'
    assert history.total == 4
    assert len(history) == 3
    assert history.head == 'bar'
    assert history.tail == 'qux'

    # Test deleting an item
    del history['qux']
    assert history.total == 3
    assert len(history) == 2
    assert history.head == 'bar'
    assert history.tail == 'baz'

    # Test clearing the history
    history.clear()
    assert history.maxsize == 3
    assert history.head is None
    assert history.tail is None
    assert history.total == 0


# Tests for always leader ``Node`` class
def test_node_is_leader():
    assert Node('node:7000').is_leader()


def test_node_has_leader():
    assert Node('node:7000').has_leader()


def test_node_property_neighbours():
    assert list(Node('node:7000').neighbours.keys()) == list()
    assert list(Node('node:7000', ['n1:7000']).neighbours.keys()) == ['n1:7000']


def test_node_property_neighbourhood():
    assert Node('node:7000').neighbourhood == ['node:7000']
    assert Node('node:7000', ['n1:7000']).neighbourhood == ['node:7000', 'n1:7000']


def test_node_property_meta():
    node = Node('node:7000')
    assert node.meta == {}

    node.meta['foo'] = 'bar'
    assert node.meta['foo'] == 'bar'


def test_node_member():
    node = Node('node:7000', ['n1:7000', 'n2:7000'])
    assert node.member(node.leader) == node
    assert node.member(node.address) == node
    assert node.member('nX:7000') is None
    assert node.member('n1:7000').address == 'n1:7000'
    assert node.member('n2:7000').address == 'n2:7000'
