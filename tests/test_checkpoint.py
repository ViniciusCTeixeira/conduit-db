from pathlib import Path
from conduit.checkpoint import CheckpointStore


def test_checkpoint_lifecycle(tmp_path: Path):
    db_file = tmp_path / "state.db"
    store = CheckpointStore(db_file)

    job_id = store.get_or_create_job("src_prod", "dst_local")
    assert job_id == "src_prod_to_dst_local"

    cp = store.init_table(job_id, "users", "id", "int", total_rows=1000)
    assert cp.status == "PENDING"
    assert cp.migrated_rows == 0

    store.update_cursor(job_id, "users", last_cursor_value=500, newly_migrated_count=500)
    updated = store.get_table_checkpoint(job_id, "users")
    assert updated is not None
    assert updated.migrated_rows == 500
    assert updated.last_cursor_value == "500"
    assert updated.status == "IN_PROGRESS"

    store.mark_completed(job_id, "users")
    completed = store.get_table_checkpoint(job_id, "users")
    assert completed is not None
    assert completed.status == "COMPLETED"


def test_checkpoint_index_tracking(tmp_path: Path):
    db_file = tmp_path / "state.db"
    store = CheckpointStore(db_file)
    job_id = store.get_or_create_job("src_prod", "dst_local")

    # Record dropped index
    store.record_dropped_index(job_id, "rfb_empresas", "idx_rfb_emp_razao", "KEY `idx_rfb_emp_razao` (`razao_social`)")
    dropped = store.get_dropped_indexes(job_id, "rfb_empresas")
    assert len(dropped) == 1
    assert dropped[0]["index_name"] == "idx_rfb_emp_razao"
    assert dropped[0]["status"] == "DROPPED"

    # Mark restored
    store.mark_index_restored(job_id, "rfb_empresas", "idx_rfb_emp_razao")
    pending = store.get_dropped_indexes(job_id, "rfb_empresas")
    assert len(pending) == 0

    all_tracked = store.list_all_indexes(job_id)
    assert len(all_tracked) == 1
    assert all_tracked[0]["status"] == "RESTORED"

    # Test get_dropped_indexes without job_id
    store.record_dropped_index(job_id, "rfb_empresas", "idx_rfb_emp_porte", "KEY `idx_rfb_emp_porte` (`porte`)")
    global_dropped = store.get_dropped_indexes(table_name="rfb_empresas")
    assert len(global_dropped) == 1
    assert global_dropped[0]["index_name"] == "idx_rfb_emp_porte"


def test_composite_checkpoint_cursor(tmp_path: Path):
    import json
    db_file = tmp_path / "state.db"
    store = CheckpointStore(db_file)
    job_id = store.get_or_create_job("src_prod", "dst_local")

    cp = store.init_table(job_id, "rfb_estabelecimentos", "cnpj_basico,cnpj_ordem,cnpj_dv", "composite", total_rows=1000)
    assert cp.pk_column == "cnpj_basico,cnpj_ordem,cnpj_dv"
    assert cp.pk_type == "composite"

    # Store composite cursor as tuple
    composite_cursor = ("00000000", "0001", "91")
    store.update_cursor(job_id, "rfb_estabelecimentos", last_cursor_value=composite_cursor, newly_migrated_count=100)

    updated = store.get_table_checkpoint(job_id, "rfb_estabelecimentos")
    assert updated is not None
    assert updated.migrated_rows == 100
    assert updated.last_cursor_value == json.dumps(list(composite_cursor))
