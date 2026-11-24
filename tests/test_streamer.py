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


def test_composite_keyset_streaming():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    batch1 = [("00000000", "0001", "91", "Empresa 1"), ("00000000", "0002", "72", "Empresa 2")]
    batch2 = [("00000001", "0001", "12", "Empresa 3")]
    mock_cursor.fetchall.side_effect = [batch1, batch2, []]

    meta = TableMeta(
        name="rfb_estabelecimentos",
        primary_key=None,
        composite_pks=["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
        columns=[
            ColumnInfo("cnpj_basico", "varchar", "varchar(8)", False, None, "", ""),
            ColumnInfo("cnpj_ordem", "varchar", "varchar(4)", False, None, "", ""),
            ColumnInfo("cnpj_dv", "varchar", "varchar(2)", False, None, "", ""),
            ColumnInfo("nome_fantasia", "varchar", "varchar(100)", True, None, "", ""),
        ],
    )

    streamer = DataStreamer(mock_conn, chunk_size=2)
    chunks = list(streamer.stream_table(meta))

    assert len(chunks) == 2
    assert chunks[0][0] == batch1
    assert chunks[0][2] == ("00000000", "0002", "72")
    assert chunks[1][0] == batch2
    assert chunks[1][2] == ("00000001", "0001", "12")

    # Verify query for 2nd batch includes composite row constructor comparison
    called_queries = [call_args[0][0] for call_args in mock_cursor.execute.call_args_list]
    assert len(called_queries) == 2
    assert "WHERE (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`) > (%s, %s, %s)" in called_queries[1]
    assert "ORDER BY `cnpj_basico` ASC, `cnpj_ordem` ASC, `cnpj_dv` ASC" in called_queries[1]
