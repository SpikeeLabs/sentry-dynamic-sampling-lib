from time import sleep
from unittest.mock import MagicMock, Mock, patch

import pytest
from requests.exceptions import RequestException

from sentry_dynamic_sampling_lib.sampler import ControllerClient, TraceSampler, on_exit
from sentry_dynamic_sampling_lib.shared import MetricType


@patch("sentry_dynamic_sampling_lib.sampler.schedule")
def test_controller_client_run_and_kill(schedule: Mock):
    c = ControllerClient(poll_interval=600, metric_interval=600)
    c.start()
    assert c.is_alive()
    sleep(8)
    assert schedule.every.call_count == 2
    schedule.run_pending.assert_called()
    c.kill()
    assert not c.is_alive()


def test_controller_client_kill_not_running():
    c = ControllerClient(poll_interval=600, metric_interval=600)
    assert not c.is_alive()
    c.join = MagicMock()
    c.kill()
    c.join.assert_not_called()


def test_controller_client_update_config():
    controller_endpoint = "/route/{}/"
    app_key = "1"
    c = ControllerClient(controller_endpoint=controller_endpoint, app_key=app_key)
    c.session = MagicMock()
    c.session.get.return_value.from_cache = False
    c.session.get.return_value.json.return_value = {
        "active_sample_rate": 20,
        "wsgi_ignore_path": [1, 2],
        "celery_ignore_task": (1, 2),
        "celery_collect_metrics": True,
        "wsgi_collect_metrics": True,
    }
    c.update_config()

    c.session.get.assert_called_once_with(controller_endpoint.format(app_key), timeout=1)
    resp = c.session.get.return_value
    resp.raise_for_status.assert_called_once_with()
    resp.json.assert_called_once_with()

    assert c.app_config.sample_rate == 20
    assert c.app_config.ignored_paths == {1, 2}
    assert c.app_config.ignored_tasks == {1, 2}

    assert c.metrics.get_mode(MetricType.CELERY)
    assert c.metrics.get_mode(MetricType.WSGI)


def test_controller_client_update_config_exception():
    controller_endpoint = "/route/{}/"
    app_key = "1"
    c = ControllerClient(controller_endpoint=controller_endpoint, app_key=app_key)
    c.session = MagicMock()
    c.session.get.side_effect = RequestException

    c.update_config()

    c.session.get.assert_called_once_with(controller_endpoint.format(app_key), timeout=1)
    resp = c.session.get.return_value
    resp.json.assert_not_called()


def test_controller_client_update_config_cache():
    controller_endpoint = "/route/{}/"
    app_key = "1"
    c = ControllerClient(controller_endpoint=controller_endpoint, app_key=app_key)
    c.session = MagicMock()
    c.session.get.return_value.from_cache = True
    c.session.get.return_value.json.return_value = {
        "active_sample_rate": 20,
        "wsgi_ignore_path": [1, 2],
        "celery_ignore_task": (1, 2),
        "celery_collect_metrics": True,
        "wsgi_collect_metrics": True,
    }
    c.update_config()

    c.session.get.assert_called_once_with(controller_endpoint.format(app_key), timeout=1)
    resp = c.session.get.return_value
    resp.raise_for_status.assert_called_once_with()
    resp.json.assert_not_called()


def test_controller_client_update_metric():
    metric_endpoint = "/route/{}/{}/"
    app_key = "1"
    c = ControllerClient(metric_endpoint=metric_endpoint, app_key=app_key)
    c.session = MagicMock()

    c.metrics.set_mode(MetricType.CELERY, True)
    c.metrics.set_mode(MetricType.WSGI, True)

    c.metrics.count_path("/test/")
    c.metrics.count_task("run")

    c.update_metrics()

    assert c.session.post.call_count == 2


def test_controller_client_update_metric_exception():
    metric_endpoint = "/route/{}/{}/"
    app_key = "1"
    c = ControllerClient(metric_endpoint=metric_endpoint, app_key=app_key)
    c.session = MagicMock()

    c.metrics.set_mode(MetricType.CELERY, True)
    c.metrics.set_mode(MetricType.WSGI, True)

    c.metrics.count_path("/test/")
    c.metrics.count_task("run")
    c.session.post.side_effect = RequestException
    c.update_metrics()

    assert c.session.post.call_count == 2


@patch("sentry_dynamic_sampling_lib.sampler.TraceSampler")
def test_on_exit(trace_sampler: Mock):
    with pytest.raises(KeyboardInterrupt):
        on_exit()
    trace_sampler.assert_called_once_with()
    trace_sampler.return_value.kill.assert_called_once_with()


@patch("sentry_dynamic_sampling_lib.sampler.signal")
@patch("sentry_dynamic_sampling_lib.sampler.worker_shutdown")
@patch("sentry_dynamic_sampling_lib.sampler.ControllerClient")
def test_trace_sampler(controller_client: Mock, worker_shutdown: Mock, signal: Mock):
    TraceSampler.clear()

    args = [1, 2]
    kwargs = {"test": 5}
    ts = TraceSampler(*args, **kwargs)

    assert ts._controller is None
    assert ts._tread_for_pid is None
    signal.signal.assert_called_once_with(signal.SIGINT, on_exit)
    worker_shutdown.connect.assert_called_once_with(on_exit)

    assert not ts.has_running_controller

    ts._ensure_controller()

    controller_client.assert_called_once_with(*args, **kwargs)
    controller_client.return_value.start.assert_called_once_with()

    controller_client.return_value.is_alive.return_value = False
    assert not ts.has_running_controller

    controller_client.return_value.is_alive.return_value = True
    assert ts.has_running_controller

    ts._tread_for_pid = 0
    assert not ts.has_running_controller

    TraceSampler.clear()

    del ts

    controller_client.return_value.kill.assert_called_once_with()


@patch("sentry_dynamic_sampling_lib.sampler.signal")
@patch("sentry_dynamic_sampling_lib.sampler.worker_shutdown")
def test_trace_sampler_no_celery_signal(worker_shutdown: Mock, signal: Mock):
    TraceSampler.clear()

    worker_shutdown.__bool__.return_value = False
    ts = TraceSampler()

    assert ts._controller is None
    assert ts._tread_for_pid is None
    signal.signal.assert_called_once_with(signal.SIGINT, on_exit)
    worker_shutdown.connect.assert_not_called()


def test_trace_sampler_singleton():
    assert TraceSampler() == TraceSampler()


@patch("sentry_dynamic_sampling_lib.sampler.ControllerClient")
def test_trace_sampler_property(controller_client: Mock):
    TraceSampler.clear()
    ts = TraceSampler()
    ts._ensure_controller()
    assert ts.app_config == controller_client.return_value.app_config
    assert ts.metrics == controller_client.return_value.metrics


@patch("sentry_dynamic_sampling_lib.sampler.ControllerClient")
def test_trace_sampler_kill(controller_client: Mock):
    TraceSampler.clear()
    ts = TraceSampler()
    ts._ensure_controller()

    ts._start_controller = MagicMock()
    ts._ensure_controller()
    ts._start_controller.assert_not_called()

    ts.kill()
    controller_client.return_value.kill.assert_called_once_with()


def test_trace_sampler_call():
    TraceSampler.clear()
    ts = TraceSampler()
    ts._ensure_controller = MagicMock()
    ts._controller = MagicMock()

    assert ts({}) == ts._controller.app_config.sample_rate

    ts._controller.app_config.ignored_paths = ["/re"]
    ctx = {"wsgi_environ": {"PATH_INFO": "/re"}}
    assert ts(ctx) == 0

    ts._controller.app_config.ignored_paths = []
    ctx = {"wsgi_environ": {"PATH_INFO": "/re"}}
    assert ts(ctx) == ts._controller.app_config.sample_rate
    ts._controller.metrics.count_path.assert_called_once_with("/re")

    ts._controller.app_config.ignored_tasks = ["/re"]
    ctx = {"celery_job": {"task": "/re"}}
    assert ts(ctx) == 0

    ts._controller.app_config.ignored_tasks = []
    ctx = {"celery_job": {"task": "/re"}}
    assert ts(ctx) == ts._controller.app_config.sample_rate
    ts._controller.metrics.count_task.assert_called_once_with("/re")
