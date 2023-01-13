# Sentry Dynamic Sampling Controller

[![PyPI](https://img.shields.io/pypi/v/sentry-dynamic-sampling-lib?color=blue)](https://pypi.org/project/sentry-dynamic-sampling-lib/)
![Tests Status](https://github.com/SpikeeLabs/sentry-dynamic-sampling-lib/actions/workflows/.github/workflows/test.yml/badge.svg)
[![codecov](https://codecov.io/gh/SpikeeLabs/sentry-dynamic-sampling-lib/branch/main/graph/badge.svg?token=NK5V6YMWW0)](https://codecov.io/gh/SpikeeLabs/sentry-dynamic-sampling-lib)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sentry-dynamic-sampling-lib)
![PyPI - License](https://img.shields.io/pypi/l/sentry-dynamic-sampling-lib)

---
This project aims to provide dynamic sampling without relying on Sentry own's Dynamic Sampling.
This libs works by adding a `traces_sampler` callback to sentry.
In the background a thread fetch the data from the [controller](https://github.com/SpikeeLabs/sentry-dynamic-sampling-controller)
It's also able to ignore WSGI route an Celery task set in controller.





## Usage
```python
import sentry_sdk
from sentry_dynamic_sampling_lib import init_wrapper

# init sentry as usual
# without traces_sampler and sample_rate param
sentry_sdk.init(  # pylint: disable=E0110
    dsn=SENTRY_DSN,
    integrations=[],
    environment=ENVIRONMENT,
    release=SENTRY_RELEASE,
)

# hook sentry_dynamic_sampling_lib into sentry
init_wrapper()
```


## Configuration
The following environment variables can be used to configure the lib

```bash
SENTRY_CONTROLLER_HOST=none # (required, no default)
SENTRY_CONTROLLER_PATH="/sentry/apps/{}/" # (optional, default to example)
SENTRY_CONTROLLER_METRIC_PATH="/sentry/apps/{}/metrics/{}/" # (optional, default to example)
SENTRY_CONTROLLER_POLL_INTERVAL=60 # (optional, default to example)
SENTRY_CONTROLLER_METRIC_INTERVAL=600 # (optional, default to example)
```




## Development
```bash
# install deps
poetry install

# pre-commit
poetry run pre-commit install --install-hook
poetry run pre-commit install --install-hooks --hook-type commit-msg
```
