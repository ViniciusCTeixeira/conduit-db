from unittest.mock import MagicMock
from conduit.inspector import DatabaseInspector


def test_topological_sort_dependencies():
    mock_conn = MagicMock()
    inspector = DatabaseInspector(mock_conn, "test_db")

    # Mock get_table_metadata to simulate orders -> users dependency
    def fake_get_meta(tbl):
        mock_meta = MagicMock()
        mock_meta.name = tbl
        if tbl == "orders":
            mock_meta.depends_on = {"users"}
        elif tbl == "order_items":
            mock_meta.depends_on = {"orders", "products"}
        else:
            mock_meta.depends_on = set()
        return mock_meta

    inspector.get_table_metadata = fake_get_meta

    tables = ["order_items", "orders", "users", "products"]
    sorted_tables = inspector.sort_tables_topologically(tables)

    # users and products must come before orders, and orders before order_items
    assert sorted_tables.index("users") < sorted_tables.index("orders")
    assert sorted_tables.index("orders") < sorted_tables.index("order_items")
    assert sorted_tables.index("products") < sorted_tables.index("order_items")


def test_get_secondary_indexes():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    ddl = """CREATE TABLE `rfb_empresas` (
  `cnpj_basico` varchar(8) NOT NULL,
  `razao_social` varchar(150) NOT NULL,
  `porte` int unsigned NOT NULL,
  PRIMARY KEY (`cnpj_basico`),
  KEY `idx_rfb_emp_razao` (`razao_social`),
  KEY `idx_rfb_emp_porte` (`porte`),
  UNIQUE KEY `idx_uniq_sample` (`cnpj_basico`, `porte`)
) ENGINE=InnoDB"""

    mock_cur.fetchone.return_value = ("rfb_empresas", ddl)
    inspector = DatabaseInspector(mock_conn, "test_db")
    indexes = inspector.get_secondary_indexes("rfb_empresas")

    assert len(indexes) == 3
    idx_names = [i.name for i in indexes]
    assert "idx_rfb_emp_razao" in idx_names
    assert "idx_rfb_emp_porte" in idx_names
    assert "idx_uniq_sample" in idx_names

    uniq = next(i for i in indexes if i.name == "idx_uniq_sample")
    assert uniq.is_unique is True
    assert "PRIMARY" not in idx_names
