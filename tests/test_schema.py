from conduit.schema import SchemaMigrator


def test_strip_foreign_keys():
    sql = """CREATE TABLE `orders` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `amount` decimal(10,2) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_orders_user` (`user_id`),
  CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""

    clean_sql, fks = SchemaMigrator.strip_foreign_keys(sql)
    assert "CONSTRAINT" not in clean_sql
    assert "FOREIGN KEY (`user_id`)" not in clean_sql
    assert len(fks) == 1
    assert "FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)" in fks[0]
    assert clean_sql.strip().endswith("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
