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
