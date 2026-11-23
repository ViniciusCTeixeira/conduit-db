from unittest.mock import MagicMock
from conduit.inspector import ColumnInfo, TableMeta
from conduit.streamer import DataStreamer


def test_keyset_streaming_chunks():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    batch1 = [(1, "Alice"), (2, "Bob")]
    batch2 = [(3, "Charlie")]
    mock_cursor.fetchall.side_effect = [batch1, batch2, []]

    meta = TableMeta(
        name="users",
        primary_key="id",
        columns=[
            ColumnInfo("id", "int", "int", False, None, "auto_increment", ""),
            ColumnInfo("name", "varchar", "varchar(50)", False, None, "", ""),
        ],
    )

    streamer = DataStreamer(mock_conn, chunk_size=2)
    chunks = list(streamer.stream_table(meta))

    assert len(chunks) == 2
    assert chunks[0][0] == batch1
    assert chunks[0][2] == 2
    assert chunks[1][0] == batch2
    assert chunks[1][2] == 3
