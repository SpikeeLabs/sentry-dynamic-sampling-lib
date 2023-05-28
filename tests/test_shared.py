from collections import Counter
from unittest.mock import MagicMock

from sentry_dynamic_sampling_lib import settings
from sentry_dynamic_sampling_lib.shared import AppConfig, Metric, MetricType


def test_config():
    c = AppConfig()
    c._lock = MagicMock()

    # lock mock
    synchronized_mock = c._lock.__enter__

    # default value
    assert c.sample_rate == settings.DEFAULT_SAMPLE_RATE
    assert c.ignored_paths == settings.DEFAULT_IGNORED_PATH
    assert c.ignored_tasks == settings.DEFAULT_IGNORED_TASK

    # assert use lock
    assert synchronized_mock.call_count == 3

    c.sample_rate = 20
    assert c.sample_rate == 20

    # assert use lock
    assert synchronized_mock.call_count == 5

    c.ignored_paths = ["a", "b"]
    assert c.ignored_paths == {"a", "b"}

    assert synchronized_mock.call_count == 7

    c.ignored_user_agents = ["a", "b"]
    assert c.ignored_user_agents == ("a", "b")

    # assert use lock
    assert synchronized_mock.call_count == 9

    c.ignored_tasks = ["a", "b"]
    assert c.ignored_tasks == {"a", "b"}

    # assert use lock
    assert synchronized_mock.call_count == 11

    data = {"active_sample_rate": 10, "wsgi_ignore_path": (1, 2), "celery_ignore_task": (3, 4)}
    c.update(data)

    # assert use lock
    assert synchronized_mock.call_count == 12

    assert c.sample_rate == 10
    assert c.ignored_paths == {1, 2}
    assert c.ignored_tasks == {3, 4}

    # assert use lock
    assert synchronized_mock.call_count == 15


def test_metric():
    m = Metric()
    m._lock = MagicMock()
    # lock mock
    synchronized_mock = m._lock.__enter__

    # getter
    assert not m.get_mode(MetricType.CELERY)
    assert not m.get_mode(MetricType.WSGI)

    # setter
    m.set_mode(MetricType.CELERY, True)
    m.set_mode(MetricType.WSGI, True)

    assert m.get_mode(MetricType.CELERY)
    assert m.get_mode(MetricType.WSGI)

    # metric
    m.count_path("/metric/")
    assert synchronized_mock.call_count == 1

    m.count_user_agent("kube/1.26")
    assert synchronized_mock.call_count == 2

    m.count_task("celery.run")
    assert synchronized_mock.call_count == 3

    # iteration
    assert list(m) == [
        (MetricType.WSGI, {"path": Counter(["/metric/"]), "user_agent": Counter(["kube/1.26"])}),
        (MetricType.CELERY, {"task": Counter(["celery.run"])}),
    ]

    assert synchronized_mock.call_count == 5

    assert list(m) == []

    assert synchronized_mock.call_count == 7

    m.set_mode(MetricType.WSGI, False)
    m.set_mode(MetricType.CELERY, False)

    assert list(m) == []
    assert synchronized_mock.call_count == 7

    m.count_path("/metric/")
    m.count_user_agent("kube/1.26")
    m.count_task("celery.run")

    assert synchronized_mock.call_count == 10
    assert list(m) == []
