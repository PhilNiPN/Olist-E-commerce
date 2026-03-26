"""
Tests for load_bronze.py: the _file_changed function that controls
whether a file gets re-loaded (idempotency gate).

WHAT THIS TESTS:
  _file_changed compares a file's current SHA-256 hash against the last hash
  stored in ingestion.file_manifest.  It returns True (reload) or False (skip).

WHY THIS MATTERS:
  This is the idempotency gate for the entire bronze layer.  If it returns True
  when nothing changed, we waste time re-loading.  If it returns False when the
  file DID change, we miss new data silently.

TECHNIQUE:
  Uses mock_conn from conftest.py to simulate fetchone results:
    - None          -> file never loaded before  -> True
    - ("old_hash",) -> hash differs              -> True
    - ("same",)     -> hash matches              -> False
  No real DB needed; the function only does one SELECT.
"""
from bronze.load_bronze import _file_changed


class TestFileChanged:
    """
    WHAT: Exercise all three branches of _file_changed.
    
    WHY:  Each branch maps to a distinct pipeline behavior (first load, reload, skip).
    
    TECHNIQUE: mock_conn(fetchone=...) controls the single SELECT result.
    """
    def test_true_when_never_loaded_before(self, mock_conn):
        """
        First load: no row in file_manifest, so file is 'changed'.
        """
        conn, _ = mock_conn(fetchone=None)
        assert _file_changed(conn, "orders.csv", "abc123") is True

    def test_true_when_hash_differs(self, mock_conn):
        """
        Source file changed since last load.
        """
        conn, _ = mock_conn(fetchone=("old_hash_value",))
        assert _file_changed(conn, "orders.csv", "new_hash_value") is True

    def test_false_when_hash_matches(self, mock_conn):
        """
        File unchanged — skip loading.
        """
        conn, _ = mock_conn(fetchone=("same_hash",))
        assert _file_changed(conn, "orders.csv", "same_hash") is False