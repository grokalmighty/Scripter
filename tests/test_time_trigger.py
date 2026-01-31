from datetime import datetime
from scheduler.triggers.time import TimeTrigger

def test_time_trigger_next_run_basic():
    t = TimeTrigger({"time": "12:24"})
    after = datetime(2026, 1, 31, 12, 0)
    nxt = t.next_run(after)
    assert nxt.hour == 12 and nxt.minute == 24

def test_time_trigger_should_not_double_run_same_day():
    t = TimeTrigger({"time": "00:00"})
    last = datetime(2026, 1, 31, 1, 0)
    assert t.should_trigger(last) is False