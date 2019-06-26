from reactor.util import expand_dict


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
