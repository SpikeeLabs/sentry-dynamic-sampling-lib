from collections import Counter
from enum import Enum
from threading import RLock

from sentry_dynamic_sampling_lib.config import (
    DEFAULT_IGNORED_PATH,
    DEFAULT_IGNORED_TASK,
    DEFAULT_SAMPLE_RATE,
)
from sentry_dynamic_sampling_lib.utils import synchronized


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


class MetricType(Enum):
    WSGI = "WSGI"
    CELERY = "CELERY"


class Metric:
    def __init__(self) -> None:
        self._lock = RLock()
        self._counters = {
            MetricType.WSGI: Counter(),
            MetricType.CELERY: Counter(),
        }

    @synchronized
    def count_path(self, path):
        self._counters[MetricType.WSGI][path] += 1

    @synchronized
    def count_task(self, path):
        self._counters[MetricType.CELERY][path] += 1

    @synchronized
    def get_and_reset(self, _type):
        counter = self._counters[_type]
        self._counters[_type] = Counter()
        return counter
