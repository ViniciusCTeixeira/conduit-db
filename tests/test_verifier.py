from unittest.mock import MagicMock
from conduit.verifier import DataVerifier


def test_verifier_match():
    mock_src = MagicMock()
    mock_dst = MagicMock()

    cur_src = MagicMock()
    cur_dst = MagicMock()

    mock_src.cursor.return_value.__enter__.return_value = cur_src
    mock_dst.cursor.return_value.__enter__.return_value = cur_dst

    cur_src.fetchone.return_value = (5000,)
    cur_dst.fetchone.return_value = (5000,)

    verifier = DataVerifier(mock_src, mock_dst)
    res = verifier.verify_table("customers", check_hash=False)

    assert res.source_count == 5000
    assert res.target_count == 5000
    assert res.count_matches is True
    assert res.error_message is None
