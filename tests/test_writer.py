from unittest.mock import MagicMock
from conduit.writer import DataWriter


def test_writer_bulk_insert():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    writer = DataWriter(mock_conn)
    rows = [(1, "Item A"), (2, "Item B")]
    inserted = writer.write_batch("products", ["id", "title"], rows, upsert=False)

    assert inserted == 2
    mock_cursor.executemany.assert_called_once()
    called_sql = mock_cursor.executemany.call_args[0][0]
    assert "INSERT INTO `products` (`id`, `title`) VALUES (%s, %s)" in called_sql
    mock_conn.commit.assert_called()


def test_writer_upsert():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    writer = DataWriter(mock_conn)
    rows = [(1, "Item A")]
    writer.write_batch("products", ["id", "title"], rows, upsert=True)

    called_sql = mock_cursor.executemany.call_args[0][0]
    assert "ON DUPLICATE KEY UPDATE `id` = VALUES(`id`), `title` = VALUES(`title`)" in called_sql


def test_writer_drop_and_recreate_index():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    writer = DataWriter(mock_conn)
    writer.drop_secondary_index("rfb_empresas", "idx_rfb_emp_razao")
    mock_cursor.execute.assert_any_call("ALTER TABLE `rfb_empresas` DROP INDEX `idx_rfb_emp_razao`")

    writer.recreate_secondary_index("rfb_empresas", "KEY `idx_rfb_emp_razao` (`razao_social`)")
    mock_cursor.execute.assert_any_call("ALTER TABLE `rfb_empresas` ADD KEY `idx_rfb_emp_razao` (`razao_social`)")


def test_writer_batch_delete():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    writer = DataWriter(mock_conn)
    deleted = writer.batch_delete("users", "id", [10, 20, 30])
    assert deleted == 3
    mock_cursor.execute.assert_called_with("DELETE FROM `users` WHERE `id` IN (%s, %s, %s)", (10, 20, 30))
