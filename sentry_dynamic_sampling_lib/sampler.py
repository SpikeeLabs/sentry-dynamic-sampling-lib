import logging
from threading import Thread
from time import sleep

import schedule
from requests.exceptions import RequestException
from requests_cache import CachedSession

from sentry_dynamic_sampling_lib.shared import Config, Metric, MetricType

LOGGER = logging.getLogger("SentryWrapper")


class ControllerClient(Thread):
    def __init__(self, config, metric, *args, **kwargs) -> None:
        self.poll_interval = kwargs.pop("poll_interval")
        self.metric_interval = kwargs.pop("metric_interval")
        self.controller_endpoint = kwargs.pop("controller_endpoint")
        self.metric_endpoint = kwargs.pop("metric_endpoint")
        self.app_key = kwargs.pop("app_key")
        self.config: Config = config
        self.metrics: Metric = metric
        self.session = CachedSession(backend="memory", cache_control=True)
        super().__init__(*args, name="SentryControllerClient", daemon=True, **kwargs)

    def run(self):
        # HACK: Django change the timezone mid startup
        # Which break the datetime.datetime.now() method
        # This then break schedule by delaying the startup by the timezone delta
        sleep(5)
        schedule.every(self.poll_interval).seconds.do(self.update_config)
        schedule.every(self.metric_interval).seconds.do(self.update_metrics)
        while True:
            schedule.run_pending()
            sleep(1)

    def update_config(self):
        try:
            resp = self.session.get(
                self.controller_endpoint.format(self.app_key), timeout=1
            )
            resp.raise_for_status()
        except RequestException as err:
            LOGGER.warning("App Request Failed: %s", err)
            return

        if resp.from_cache:
            return

        data = resp.json()
        self.config.sample_rate = data["active_sample_rate"]
        self.config.ignored_paths = data["wsgi_ignore_path"]
        self.config.ignored_tasks = data["celery_ignore_task"]

    def update_metrics(self):
        for metric_type in MetricType:
            counter = self.metrics.get_and_reset(metric_type)
            data = {
                "app": self.app_key,
                "type": metric_type.value,
                "data": dict(counter.most_common(10)),
            }
            try:
                self.session.post(
                    self.metric_endpoint.format(self.app_key, metric_type.value),
                    json=data,
                )
            except RequestException as err:
                LOGGER.warning("Metric Request Failed: %s", err)
                return


class TraceSampler:
    def __init__(self, *args, **kwargs) -> None:
        self.config = Config()
        self.metrics = Metric()
        self.controller = ControllerClient(*args, self.config, self.metrics, **kwargs)
        self.controller.start()

    def __call__(self, sampling_context):
        if sampling_context:
            if "wsgi_environ" in sampling_context:
                path = sampling_context["wsgi_environ"].get("PATH_INFO", "")
                if path in self.config.ignored_paths:
                    return 0
                self.metrics.count_path(path)
            if "celery_job" in sampling_context:
                task = sampling_context["celery_job"].get("task", "")
                if task in self.config.ignored_tasks:
                    return 0
                self.metrics.count_task(task)
        return self.config.sample_rate
