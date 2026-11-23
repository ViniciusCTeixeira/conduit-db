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
