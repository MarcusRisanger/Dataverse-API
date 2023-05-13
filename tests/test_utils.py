from dataverse_api.utils import DataverseBatchCommand, chunk_data, extract_key, batch_id_generator


def test_chunk_data():
    data = [
        DataverseBatchCommand(uri="uri1", mode="mode1", data={"col1": 1, "col2": 2}),
        DataverseBatchCommand(uri="uri2", mode="mode1", data={"col1": 3, "col2": 4}),
        DataverseBatchCommand(uri="uri3", mode="mode1", data={"col1": 5, "col2": 6}),
    ]
    data_size = len(data)

    assert data_size == 3

    size = 2
    a = chunk_data(data, size=size)

    first = a.__next__()

    assert len(first) == size
    assert first[0].data == {"col1": 1, "col2": 2}
    assert first[1].data == {"col1": 3, "col2": 4}

    second = a.__next__()

    assert len(second) == data_size - len(first)
    assert second[0].data == {"col1": 5, "col2": 6}


def test_extract_key():
    key_columns = {"a"}

    data = {"a": 1, "b": 2, "c": 3}
    result = extract_key(data, key_columns)

    assert result == "a=1"
    assert data == {"b": 2, "c": 3}

    data = {"a": "abc", "b": 2, "c": 3}
    result = extract_key(data, key_columns)

    assert result == "a='abc'"
    assert data == {"b": 2, "c": 3}

    key_columns = {"a", "b"}
    data = {"a": "abc", "b": 2, "c": 3, "d": "hello"}

    result = extract_key(data, key_columns)

    assert result == "a='abc',b=2"
    assert data == {"c": 3, "d": "hello"}


def test_batch_id_generator():
    for _ in range(10):
        id = batch_id_generator()

        assert len(id) == 36
        assert str(id)[8] == "-"
        assert str(id)[13] == "-"
        assert str(id)[14] == "4"
