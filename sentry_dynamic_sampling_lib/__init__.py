import importlib
import logging
import os
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

import psutil

from sentry_dynamic_sampling_lib.config import (
    CONTROLLER_HOST,
    CONTROLLER_PATH,
    METRIC_INTERVAL,
    METRIC_PATH,
    POLL_INTERVAL,
)
from sentry_dynamic_sampling_lib.sampler import TraceSampler

if TYPE_CHECKING:
    import sentry_sdk as sentry_sdk_type

LOGGER = logging.getLogger("SentryWrapper")


def build_app_key(options):
    dsn = options.get("dsn")
    env = options.get("environment")
    path = urlparse(dsn).path
    project_id = os.path.split(path)[-1]
    process = psutil.Process(os.getpid())

    project_id = project_id.replace("_", "-")
    env = env.replace("_", "-")
    process_name = process.name().replace("_", "-")

    return f"{project_id}_{env}_{process_name}"


def init_wrapper():
    if not importlib.util.find_spec("sentry_sdk"):
        return

    sentry_sdk: sentry_sdk_type = importlib.import_module("sentry_sdk")
    client = sentry_sdk.Hub.current.client

    controller_host = CONTROLLER_HOST
    controller_path = CONTROLLER_PATH
    metric_path = METRIC_PATH
    poll_interval = POLL_INTERVAL
    metric_interval = METRIC_INTERVAL

    if controller_host:
        app_key = build_app_key(client.options)
        controller_endpoint = urljoin(controller_host, controller_path)
        metric_endpoint = urljoin(controller_host, metric_path)
        print(f"Sentry Wrapper: Injecting TracesSampler. App Key : {app_key}")
        client.options["traces_sampler"] = TraceSampler(
            poll_interval=poll_interval,
            metric_interval=metric_interval,
            metric_endpoint=metric_endpoint,
            controller_endpoint=controller_endpoint,
            app_key=app_key,
        )
