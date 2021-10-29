import concurrent.futures
import sys
from typing import Dict, Optional, Union

import sentry_sdk

from datadog import initialize, statsd

from consoleme.config import config
from consoleme.default_plugins.plugins.metrics.base_metric import Metric

log = config.get_logger()


def log_metric_error(future):
    try:
        future.result()
    except Exception as e:
        log.error(
            {
                "function": f"{__name__}.{sys._getframe().f_code.co_name}",
                "message": "Error sending metric",
                "error": str(e),
            },
            exc_info=True,
        )
        sentry_sdk.capture_exception()


class DatadogMetric(Metric):
    def __init__(self):
        self.options = {
            'statsd_host': config.get("metrics.datadog.host", "localhost"),
            'statsd_port': config.get("metrics.datadog.port", "8125")
        }
        self.prefix = config.get("metrics.datadog.prefix", "consoleme")
        self.executor = concurrent.futures.ThreadPoolExecutor(
            config.get("metrics.datadog.max_threads", 10)
        )
        initialize(**self.options)

    def format_tags(self, tags):
        dd_tags = []
        if not tags:
            return dd_tags

        for d in tags:
            if not tags[d] is None:
                dd_tags.append(d + ":" + tags[d])
                return dd_tags

    def statsd_args(self, metric_name, tags):
        return {
            "metric": self.metric_with_prefix(metric_name),
            "tags": self.format_tags(tags)
        }

    def metric_with_prefix(self, metric_name):
        # In case the metric name already comes with the consoleme pkg name as prefix,
        # we avoid duplications if the prefix has the same name
        if metric_name.split(".")[0] == self.prefix:
            return metric_name
        else:
            return self.prefix + "." + metric_name

    def count(self, metric_name, metric_value=1, tags=None):
        kwargs = self.statsd_args(metric_name, tags)

        # The count method is not supported in Dogstatsd for Python

        future = self.executor.submit(statsd.increment, **kwargs)
        future.add_done_callback(log_metric_error)

    def gauge(self, metric_name, metric_value, tags=None):
        kwargs = self.statsd_args(metric_name, tags).update({"value": metric_value})

        future = self.executor.submit(statsd.gauge, **kwargs)
        future.add_done_callback(log_metric_error)

    def timer(
            self,
            metric_name: str,
            tags: Optional[Union[Dict[str, Union[str, bool]], Dict[str, str]]] = None,
    ) -> None:
        kwargs = self.statsd_args(metric_name, tags)

        # DogStatsd Counts are stored as RATE (Datadog In-App Type).
        future = self.executor.submit(statsd.increment, **kwargs)
        future.add_done_callback(log_metric_error)
