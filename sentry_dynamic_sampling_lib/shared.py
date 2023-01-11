import logging
from collections import Counter
from enum import Enum
from threading import RLock

from sentry_dynamic_sampling_lib.config import (
    DEFAULT_IGNORED_PATH,
    DEFAULT_IGNORED_TASK,
    DEFAULT_SAMPLE_RATE,
)
from sentry_dynamic_sampling_lib.utils import synchronized

LOGGER = logging.getLogger("SentryWrapper")


class Config:
    def __init__(self) -> None:
        self._lock = RLock()
        self._sample_rate = DEFAULT_SAMPLE_RATE
        self._ignored_paths = DEFAULT_IGNORED_PATH
        self._ignored_tasks = DEFAULT_IGNORED_TASK

    @property
    @synchronized
    def sample_rate(self):
        return self._sample_rate

    @sample_rate.setter
    @synchronized
    def sample_rate(self, new_sample_rate):
        self._sample_rate = new_sample_rate

    @property
    @synchronized
    def ignored_paths(self):
        return self._ignored_paths

    @ignored_paths.setter
    @synchronized
    def ignored_paths(self, new_ignored_paths):
        self._ignored_paths = set(new_ignored_paths)

    @property
    @synchronized
    def ignored_tasks(self):
        return self._ignored_tasks

    @ignored_tasks.setter
    @synchronized
    def ignored_tasks(self, new_ignored_tasks):
        self._ignored_tasks = set(new_ignored_tasks)

    @synchronized
    def update(self, data):
        self._sample_rate = data["active_sample_rate"]
        self._ignored_paths = data["wsgi_ignore_path"]
        self._ignored_tasks = data["celery_ignore_task"]


class MetricType(Enum):
    WSGI = "WSGI"
    CELERY = "CELERY"


class Metric:
    def __init__(self) -> None:
        self._lock = RLock()
        self._metrics = {
            MetricType.WSGI: {"activated": False, "data": Counter()},
            MetricType.CELERY: {"activated": False, "data": Counter()},
        }

    def set_mode(self, _type, mode):
        self._metrics[_type]["activated"] = mode

    def get_mode(self, _type):
        return self._metrics[_type]["activated"]

    @synchronized
    def count_path(self, path):
        metric = self._metrics[MetricType.WSGI]
        if metric["activated"]:
            metric["data"][path] += 1

    @synchronized
    def count_task(self, path):
        metric = self._metrics[MetricType.CELERY]
        if metric["activated"]:
            metric["data"][path] += 1

    def __iter__(self):
        """
        List activated non-empty metrics

        Yields:
            Tuple(MetricType, Counter): the activated non-empty metrics
        """
        for metric_type, metric in self._metrics.items():
            # check if metric is activated
            if not metric["activated"]:
                LOGGER.debug("Metric %s disabled", metric_type.value)
            with self._lock:
                # check im metric is empty
                if len(metric["data"]) == 0:
                    LOGGER.debug("Metric %s is empty", metric_type.value)
                    continue
                data = metric["data"]
                metric["data"] = Counter()

            # yield outside of the lock to not block write while callee execute
            yield metric_type, data
