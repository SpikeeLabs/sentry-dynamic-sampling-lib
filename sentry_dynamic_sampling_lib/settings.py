import os

# default value overridden by controller
DEFAULT_IGNORED_PATH = {"/health", "/healthz", "/health/", "/healthz/"}
DEFAULT_IGNORED_TASK = set()
DEFAULT_SAMPLE_RATE = 0.0

# controller variables
CONTROLLER_HOST = os.getenv("SENTRY_CONTROLLER_HOST")
CONTROLLER_PATH = os.getenv("SENTRY_CONTROLLER_PATH", "/sentry/apps/{}/")
METRIC_PATH = os.getenv("SENTRY_CONTROLLER_METRIC_PATH", "/sentry/apps/{}/metrics/{}/")
POLL_INTERVAL = int(os.getenv("SENTRY_CONTROLLER_POLL_INTERVAL", "60"))
METRIC_INTERVAL = int(os.getenv("SENTRY_CONTROLLER_METRIC_INTERVAL", "600"))
