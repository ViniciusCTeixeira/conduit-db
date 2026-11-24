from unittest.mock import MagicMock
from conduit.inspector import ColumnInfo, TableMeta
from conduit.synchronizer import DataSynchronizer, TableSyncStats


def make_sample_meta():
    cols = [
        ColumnInfo("id", "int", "int(11)", False, None, "auto_increment", ""),
        ColumnInfo("name", "varchar", "varchar(100)", False, None, "", ""),
        ColumnInfo("status", "varchar", "varchar(20)", False, None, "", ""),
    ]
    return TableMeta(name="sample_table", primary_key="id", pk_type="int", columns=cols)


def test_synchronizer_identical_block_skips():
    mock_src = MagicMock()
    mock_tgt = MagicMock()
    mock_src_cur = MagicMock()
    mock_tgt_cur = MagicMock()

    mock_src.cursor.return_value.__enter__.return_value = mock_src_cur
    mock_tgt.cursor.return_value.__enter__.return_value = mock_tgt_cur

    # 1. First query: fetch PKs (100 rows)
    pks = [(i,) for i in range(1, 101)]
    # 2. Second query: block hash check -> identical count and hash
    mock_src_cur.fetchall.side_effect = [pks, []]
    mock_src_cur.fetchone.return_value = (100, "12345678")
    mock_tgt_cur.fetchone.return_value = (100, "12345678")

    sync = DataSynchronizer(mock_src, mock_tgt, chunk_size=100)
    meta = make_sample_meta()
    stats = sync.sync_table(meta)

    assert stats.unchanged == 100
    assert stats.inserted == 0
    assert stats.updated == 0
    assert stats.deleted == 0


def test_synchronizer_diff_detection_and_apply():
    mock_src = MagicMock()
    mock_tgt = MagicMock()
    mock_src_cur = MagicMock()
    mock_tgt_cur = MagicMock()

    mock_src.cursor.return_value.__enter__.return_value = mock_src_cur
    mock_tgt.cursor.return_value.__enter__.return_value = mock_tgt_cur

    # Source has rows 1 (unchanged), 2 (modified), 3 (new)
    pks = [(1,), (2,), (3,)]
    mock_src_cur.fetchall.side_effect = [
        pks,  # pks list
        [  # diff query on source: id, row_crc, id, name, status
            (1, "1111", 1, "Alice", "active"),
            (2, "2222_new", 2, "Bob Updated", "active"),
            (3, "3333", 3, "Charlie", "pending"),
        ],
        [],  # second pk loop empty
    ]
    # Block hash check returns differing hash
    mock_src_cur.fetchone.return_value = (3, "hash_src")
    mock_tgt_cur.fetchone.return_value = (3, "hash_tgt")

    # Target diff query: id, row_crc
    # Target has row 1 (hash "1111"), row 2 (hash "2222_old"), row 4 (missing in source -> to prune)
    mock_tgt_cur.fetchall.side_effect = [
        [
            (1, "1111"),
            (2, "2222_old"),
            (4, "4444"),
        ]
    ]

    # Global prune pass: min/max on source
    mock_src_cur.fetchone.side_effect = [
        (3, "hash_src"),  # block check
        (1, 3),  # global min/max
    ]

    sync = DataSynchronizer(mock_src, mock_tgt, chunk_size=10, prune=True, dry_run=False)
    meta = make_sample_meta()
    stats = sync.sync_table(meta)

    assert stats.unchanged == 1
    assert stats.updated == 1  # Bob Updated
    assert stats.inserted == 1  # Charlie
    assert stats.deleted == 1  # Row 4 pruned


def test_synchronizer_dry_run_does_not_write():
    mock_src = MagicMock()
    mock_tgt = MagicMock()
    mock_src_cur = MagicMock()
    mock_tgt_cur = MagicMock()

    mock_src.cursor.return_value.__enter__.return_value = mock_src_cur
    mock_tgt.cursor.return_value.__enter__.return_value = mock_tgt_cur

    pks = [(1,)]
    mock_src_cur.fetchall.side_effect = [
        pks,
        [(1, "new_crc", 1, "Alice", "active")],
        [],
    ]
    mock_src_cur.fetchone.return_value = (1, "src_hash")
    mock_tgt_cur.fetchone.return_value = (0, None)  # Target empty
    mock_tgt_cur.fetchall.return_value = []

    sync = DataSynchronizer(mock_src, mock_tgt, chunk_size=10, dry_run=True)
    meta = make_sample_meta()
    stats = sync.sync_table(meta)

    assert stats.inserted == 1
    # Check that executemany was NOT called on target cursor
    mock_tgt_cur.executemany.assert_not_called()
