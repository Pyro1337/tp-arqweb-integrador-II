import pytest
from benchmark_exports import measure


def test_measure_uses_observed_completion_and_actual_elapsed_time():
    times = iter([10.0, 10.1, 15.0, 15.0])
    result = measure(lambda: "job-a", lambda j: {"status": "done"},
                     lambda: next(times), lambda n: None, 1, 20, ["worker-1"])
    assert result["completed"] == 1
    assert result["elapsed_seconds"] == 5.0
    assert result["jobs_per_minute"] == 12.0
    assert result["replicas_observed"] == 1
    assert result["worker_container_ids"] == ["worker-1"]


def test_timeout_never_reports_pending_jobs_as_done():
    times = iter([0.0, 1.0, 11.0, 11.0])
    result = measure(lambda: "job-a", lambda j: {"status": "running"},
                     lambda: next(times), lambda n: None, 1, 10, ["worker-1"])
    assert result["completed"] == 0
    assert result["pending"] == 1
    assert result["passed"] is False


def test_duplicate_submission_id_is_rejected():
    with pytest.raises(ValueError):
        measure(lambda: "same", lambda j: {"status": "done"}, lambda: 0.0,
                lambda n: None, 2, 10, ["worker-1"])
