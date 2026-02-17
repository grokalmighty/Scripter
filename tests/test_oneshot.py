from datetime import datetime, timezone, timedelta

from src.database import Database
from src.scripts_repo import add_script
from src.oneshots_repo import add_one_shot, claim_due_one_shots

def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()

def test_claim_due_one_shot_once(tmp_path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    script_id = add_script(db, name="test", command="echo test")

    now = datetime.now(timezone.utc).replace(microsecond=0)
    run_at = now - timedelta(seconds=5)

    oid = add_one_shot(db, script_id=script_id, run_at_utc_iso=_iso(run_at), tz="America/New_York")
    assert oid > 0

    claimed1 = claim_due_one_shots(db, now_utc_iso=_iso(now))
    assert len(claimed1) == 1
    assert claimed1[0]["id"] == oid 
    assert claimed1[0]["script_id"] == script_id

    claimed2 = claim_due_one_shots(db, now_utc_iso=_iso(now))
    assert claimed2 == []

def test_one_shot_not_claimed_before_due(tmp_path):
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    script_id = add_script(db, name="test", command="echo test")

    now = datetime.now(timezone.utc).replace(microsecond=0)
    run_at = now +timedelta(minutes=10)

    oid = add_one_shot(db, script_id=script_id, run_at_utc_iso=_iso(run_at), tz="America/New_York")
    assert oid > 0

    claimed = claim_due_one_shots(db, now_utc_iso=_iso(now))
    assert claimed == []