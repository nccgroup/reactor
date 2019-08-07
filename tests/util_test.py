from reactor.util import expand_dict, semantic_at_least


def test_expend_dict():
    dct = expand_dict({'one.two.three': 'four'})
    assert dct == {'one': {'two': {'three': 'four'}}}


def test_expand_dict_with_overlap():
    dct = expand_dict({'parent.one': 'val_one', 'parent.two.three': 'four'})
    assert dct == {'parent': {'one': 'val_one', 'two': {'three': 'four'}}}


def test_expand_dict_partial_with_overlap():
    dct = expand_dict({'parent': {'one': 'val_one'}, 'parent.two.three': 'four'})
    assert dct == {'parent': {'one': 'val_one', 'two': {'three': 'four'}}}


def test_expand_dict_array():
    dct = expand_dict([{'one.two': 'three'}, {'four.five': 'six'}])
    assert dct == [{'one': {'two': 'three'}}, {'four': {'five': 'six'}}]


def test_expand_dict_array_with_integer_keys():
    dct = expand_dict({'alpha.0': 'one', 'alpha.1': 'two'})
    assert dct == {'alpha': ['one', 'two']}


def test_expand_dict_array_with_integer_keys_and_overlap():
    dct = expand_dict({'alpha.0.bar': 'one', 'alpha.0.baz': 'two', 'alpha.1.bar': 'three'})
    assert dct == {'alpha': [{'bar': 'one', 'baz': 'two'}, {'bar': 'three'}]}


def test_semantic_at_least_major():
    assert semantic_at_least((7, 0, 0), 6) is True
    assert semantic_at_least((6, 0, 0), 6) is True
    assert semantic_at_least((5, 0, 0), 6) is False


def test_semantic_at_least_minor():
    assert semantic_at_least((7, 2, 0), 6, 3) is True
    assert semantic_at_least((6, 4, 0), 6, 3) is True
    assert semantic_at_least((6, 3, 0), 6, 3) is True
    assert semantic_at_least((6, 2, 0), 6, 3) is False
    assert semantic_at_least((5, 6, 0), 6, 3) is False


def test_semantic_at_least_patch():
    assert semantic_at_least((7, 2, 0), 6, 3, 2) is True
    assert semantic_at_least((7, 4, 0), 6, 3, 2) is True
    assert semantic_at_least((6, 3, 3), 6, 3, 2) is True
    assert semantic_at_least((6, 3, 2), 6, 3, 2) is True
    assert semantic_at_least((6, 3, 1), 6, 3, 2) is False
    assert semantic_at_least((6, 2, 6), 6, 3, 2) is False
    assert semantic_at_least((5, 2, 6), 6, 3, 2) is False
