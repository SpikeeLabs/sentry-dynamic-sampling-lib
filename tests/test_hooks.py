from unittest.mock import Mock, patch

import pytest

import sentry_dynamic_sampling_lib as lib_root
from sentry_dynamic_sampling_lib import build_app_key, init_wrapper


@patch("sentry_dynamic_sampling_lib.psutil")
@pytest.mark.parametrize("env,project_id,process", [("production", "8528966559", "python"), ("dev", "1235", "celery")])
def test_get_app_key(psutil_mock: Mock, env, project_id, process):
    options = {"dsn": f"http://test@test.test.fr/{project_id}", "environment": env}

    psutil_mock.Process.return_value.name.return_value = process
    app_key = build_app_key(options)

    assert app_key == f"{project_id}_{env}_{process}"


@patch("sentry_dynamic_sampling_lib.importlib")
def test_init_wrapper_no_sentry(importlib_mock: Mock):
    importlib_mock.util.find_spec.return_value = False

    init_wrapper()
    importlib_mock.util.find_spec.assert_called_once_with("sentry_sdk")
    importlib_mock.import_module.assert_not_called()


@patch("sentry_dynamic_sampling_lib.TraceSampler")
@patch("sentry_dynamic_sampling_lib.importlib")
def test_init_wrapper_no_controller(importlib_mock: Mock, trace_sampler: Mock):
    importlib_mock.util.find_spec.return_value = True
    lib_root.CONTROLLER_HOST = False

    init_wrapper()
    importlib_mock.util.find_spec.assert_called_once_with("sentry_sdk")
    importlib_mock.import_module.assert_called_once_with("sentry_sdk")
    trace_sampler.assert_not_called()


@patch("sentry_dynamic_sampling_lib.build_app_key")
@patch("sentry_dynamic_sampling_lib.TraceSampler")
@patch("sentry_dynamic_sampling_lib.importlib")
def test_init_wrapper(importlib_mock: Mock, trace_sampler: Mock, build_app_key: Mock):
    importlib_mock.util.find_spec.return_value = True
    lib_root.CONTROLLER_HOST = "http://test.com"
    sentry_sdk = importlib_mock.import_module.return_value
    client = sentry_sdk.Hub.current.client
    client.options = {}
    app_key = "app_key"
    build_app_key.return_value = app_key

    init_wrapper()
    importlib_mock.util.find_spec.assert_called_once_with("sentry_sdk")
    importlib_mock.import_module.assert_called_once_with("sentry_sdk")
    build_app_key.assert_called_once_with(client.options)
    trace_sampler.assert_called_once_with(
        poll_interval=lib_root.POLL_INTERVAL,
        metric_interval=lib_root.METRIC_INTERVAL,
        metric_endpoint=f"{lib_root.CONTROLLER_HOST}{lib_root.METRIC_PATH}",
        controller_endpoint=f"{lib_root.CONTROLLER_HOST}{lib_root.CONTROLLER_PATH}",
        app_key=app_key,
    )

    assert client.options["traces_sampler"] == trace_sampler.return_value
