#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass, asdict, field, is_dataclass
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
import json
import yaml
import pytz
import logging
import time
import re

from prometheus_client import Histogram, Counter

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

logger = logging.getLogger(__name__)


# Default Parameters
# *******************************************************************
PIPELINE_CONFIG_DEFAULT = "config/metric_pipelines.toml"

# Helper Functions
# *******************************************************************
TIMEZONE_CACHE = {}  # Added to help improve performance


def load_yaml_file(filepath):
    with open(filepath, "r") as file:
        return yaml.safe_load(file)


def load_toml_file(filepath):
    with open(filepath, mode="rb") as fp:
        return tomllib.load(fp)


def shorten_data(data: str, max_length: int = 75) -> str:
    """Shorten data to a maximum length."""
    if not isinstance(data, str):
        data = str(data)
    data = data.strip()
    return data[:max_length] + "..." if len(data) > max_length else data


# Helper Functions
# *******************************************************************


def expand_metric_fields(original_dict):

    metrics_expanded = []

    for field, value in original_dict["fields"].items():
        # Start with a shallow copy of the original dict to preserve all top-level fields
        new_dict = original_dict.copy()
        # Deep copy nested dicts to avoid reference issues
        if "tags" in new_dict and isinstance(new_dict["tags"], dict):
            new_dict["tags"] = new_dict["tags"].copy()
        # Override fields with single field
        new_dict["fields"] = {field: value}
        metrics_expanded.append(new_dict)

    return metrics_expanded


def expand_metrics(metrics):
    expanded_metrics = []
    for metric in metrics:
        if is_dataclass(metric):
            metric = asdict(metric)
        if not isinstance(metric, dict):
            message = (
                "Metric is not dict, convert to a dict before using this processor"
            )
            logger.error(message)
            raise TypeError(message)
        expanded_metric = expand_metric_fields(metric)
        expanded_metrics.extend(expanded_metric)
    return expanded_metrics


def get_timezone(timezone_str):
    """
    Get a timezone object from cache or create and cache it if not found
    """
    if timezone_str not in TIMEZONE_CACHE:
        TIMEZONE_CACHE[timezone_str] = pytz.timezone(timezone_str)
    return TIMEZONE_CACHE[timezone_str]


def localize_timestamp(timestamp, timezone_str="UTC", offset=(0, 0, 0)) -> datetime:
    """
    Localize a timestamp to a timezone
    :param timestamp: The timestamp to localize
    :param timezone_str: The timezone to localize to
    :return: The localized timestamp
    """
    # Convert to datetime if not already
    if isinstance(timestamp, (int, float)):
        dt_utc = datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, datetime):
        dt_utc = timestamp
    else:
        raise ValueError("timestamp must be a float, int, or datetime object")

    # Apply offset in the form (0,0,0) representing (hours, minutes, seconds)
    dt_utc = dt_utc + timedelta(hours=offset[0], minutes=offset[1], seconds=offset[2])

    # Retrieve timezone. Previously used timezones are cached
    timezone = get_timezone(timezone_str)

    return timezone.localize(dt_utc).timestamp()


def precompile_regex_keys(formats):
    return {key: re.compile(key) for key in formats.keys()}


def check_metric_fields_length(metric):
    if len(metric["fields"]) > 1:
        logging.error(
            "Metric has more than one field. Run FieldExpander before Formatter",
            extra={"metric": metric},
        )
        raise ValueError(
            "Metric has more than one field. Run FieldExpander before Formatter"
        )


def get_metric_id(metric):
    return metric["tags"].get("id", next(iter(metric["fields"])))


def deep_merge(dict1, dict2):
    """
    Recursively merge two dictionaries.

    :param dict1: The first dictionary.
    :param dict2: The second dictionary, whose values will overwrite those in dict1 in case of conflicts.
    :return: A new dictionary that is the result of deeply merging dict2 into dict1.
    """
    merged = dict1.copy()  # Create a copy of dict1 to avoid mutating it

    for key, value in dict2.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            # If the key exists in both dictionaries and both values are dicts, merge them recursively
            merged[key] = deep_merge(merged[key], value)
        else:
            # Otherwise, set or overwrite the value in the merged dictionary
            merged[key] = value

    return merged


def build_metric_format(formats, formats_compiled, metric_id, combine=False):
    # First try to find a direct match
    format = formats.get(metric_id, {})

    if format and not combine:
        return format

    # Iterate through all formats and use regex to match each to the metric_id
    for key, regex in formats_compiled.items():
        if regex.match(metric_id):
            if not combine:
                return formats[key]
            else:
                # Merge all matching formats
                format = deep_merge(formats[key], format)
    return format


# Dataclasses
# *******************************************************************
@dataclass
class MetricStats:
    name: str
    value: dict = field(
        default_factory=dict(
            mean=None,
            max=None,
            min=None,
            count=None,
            std=None,
            sum=None,
        )
    )
    time: datetime = None

    def __iter__(self):
        yield from asdict(self).values()


# Pipeline Classes
# *******************************************************************


class MetricsPipeline(ABC):

    processing_time = Histogram(
        "metrics_processor_processing_time",
        "Average time taken to process a metric",
        ["agent", "pipeline"],
    )

    metrics_processed = Counter(
        "metrics_processed_pipeline",
        "Number of metrics processed",
        ["agent", "pipeline"],
    )

    metrics_filtered = Counter(
        "metricsprocessor_metrics_filtered",
        "Number of metrics filtered out",
        ["agent", "pipeline", "id", "reason"],
    )

    def __init__(self, config=None) -> None:

        if config:
            self._external_config = False
            self.config = config
        else:
            self._external_config = True
            self.config = self._load_config(PIPELINE_CONFIG_DEFAULT)

    def refresh_config(self):
        if self._external_config:
            self.config = self._load_config(PIPELINE_CONFIG_DEFAULT)

    def _load_config(self, filepath):
        class_name = self.__class__.__name__
        try:
            return load_toml_file(filepath)[class_name]
        except KeyError:
            logger.debug(f"No configuration specified for class {class_name}")
            return None

    def process(self, metrics):
        start_time = time.perf_counter()
        number_of_metrics = len(metrics)
        self.refresh_config()

        metrics = self.remove_none(metrics)

        if metrics:
            results = self.process_method(metrics)
        else:
            logger.debug(
                f"No metrics to process in {self.__class__.__name__}. Continuing"
            )
            return None

        end_time = time.perf_counter()

        if number_of_metrics != 0:
            self.processing_time.labels(
                agent="metrics_processor", pipeline=self.__class__.__name__
            ).observe((end_time - start_time) / number_of_metrics)
            self.metrics_processed.labels(
                agent="metrics_processor", pipeline=self.__class__.__name__
            ).inc(number_of_metrics)
        return results

    @abstractmethod
    def process_method(self, metrics): ...

    def __repr__(self):
        return self.__class__.__name__

    def remove_none(self, metrics):
        # Remove all None values from metrics
        number_metrics_initial = len(metrics)
        metrics = [metric for metric in metrics if metric is not None]
        number_metrics_final = len(metrics)
        self.metrics_filtered.labels(
            agent="metrics_processor",
            pipeline=self.__class__.__name__,
            id="None",
            reason="Invalid metric",
        ).inc(number_metrics_initial - number_metrics_final)
        return metrics


class AggregateStatistics(MetricsPipeline):
    def process_method(self, metrics):
        df = pd.DataFrame(metrics).set_index("name")
        df_mean = df.groupby("name").mean()
        df_time = df_mean.drop(columns=["value"])
        df_notime = df.drop(columns=["time"]).groupby("name")

        mean = df_mean.drop(columns=["time"]).rename(columns={"value": "mean"})
        max = df_notime.max().rename(columns={"value": "max"})
        min = df_notime.min().rename(columns={"value": "min"})
        count = df_notime.count().rename(columns={"value": "count"})
        std = df_notime.std().rename(columns={"value": "std"})
        sum = df_notime.sum().rename(columns={"value": "sum"})

        metrics_stats_dict = pd.concat(
            [mean, max, min, count, std, sum],
            axis=1,
        ).to_dict(orient="index")

        metrics_stats = [
            MetricStats(name=k, value=v, time=df_time.loc[k, "time"])
            for k, v in metrics_stats_dict.items()
        ]

        return metrics_stats


class JSONReader(MetricsPipeline):

    def process_method(self, metrics):
        for i, metric in enumerate(metrics):
            if isinstance(metric, str):
                metrics[i] = json.loads(metric)
        return metrics


class ExtraTagger(MetricsPipeline):
    # NOTE This pipeline is deprecated by Formatter
    # Use a formatter configuration with wildcards to add tags

    def process_method(self, metrics):

        tags_extra = self.config

        for metric in metrics:
            metric["tags"] = metric["tags"] | tags_extra

        return metrics


class TimeLocalizer(MetricsPipeline):

    def process_method(self, metrics):
        self.local_tz = self.config["local_tz"]
        for metric in metrics:
            # logger.debug("TimeLocalizer: Raw time is %s", metric["time"])
            local_time = localize_timestamp(
                metric["time"], timezone_str=self.local_tz, offset=self.config["offset"]
            )
            # if local_time differs by more than 59 minutes from actual local time, then offset by one hour using datime.timedelta
            if abs(local_time - int(time.time())) > 3540:
                reverse_offset = [-offset for offset in self.config["offset"]]
                local_time = datetime.fromtimestamp(local_time) + timedelta(
                    hours=reverse_offset[0],
                    minutes=reverse_offset[1],
                    seconds=reverse_offset[2],
                )
                local_time = local_time.timestamp()
            metric["time"] = local_time
        return metrics


class TimePrecision(MetricsPipeline):
    # NOTE that this pipeline implements a quick fix to an issue with RTC timestamps
    # It should be removed for future versions
    def process_method(self, metrics):
        current_time = time.time()
        for metric in metrics:
            metric_time = metric["time"]
            if metric_time > current_time + 60:
                metric["time"] = (
                    current_time  # Set to current time if it's ahead by more than a minute
                )
            else:
                metric["time"] = metric_time
        return metrics


class FieldExpander(MetricsPipeline):

    def process_method(self, metrics):
        metrics = expand_metrics(metrics)
        return metrics


class FieldToTagMapper(MetricsPipeline):
    """
    Transform field names to field+tag combinations based on expansion_pattern.

    This pipeline stage applies regex-based transformations to field names,
    extracting portions of the field name into tags while standardizing the field name.

    Example:
        Input metric:
            fields: {"DUTY_CYCLE_1": 45.2}
            expansion_pattern:
                pattern: "^DUTY_CYCLE_(\\d+)$"
                field_name: "DUTY_CYCLE"
                extract_tags:
                    - name: "channel"
                      group: 1

        Output metric:
            fields: {"DUTY_CYCLE": 45.2}
            tags: {...existing_tags..., "channel": "1"}
    """

    def process_method(self, metrics):
        result = []
        for metric in metrics:
            # Check if this metric has an expansion_pattern
            expansion_pattern = metric.get('expansion_pattern')
            if expansion_pattern:
                metric = self._apply_expansion_pattern(metric, expansion_pattern)
                # Remove expansion_pattern from metric after applying
                metric.pop('expansion_pattern', None)
            result.append(metric)
        return result

    def _apply_expansion_pattern(self, metric: dict, pattern_config: dict) -> dict:
        """
        Apply the expansion pattern to transform field names into field+tag combinations.

        Args:
            metric: The metric dict with fields, tags, measurement, time, etc.
            pattern_config: Dict with keys: pattern, field_name, extract_tags

        Returns:
            Modified metric dict with transformed fields and updated tags
        """
        pattern_str = pattern_config.get('pattern')
        new_field_name = pattern_config.get('field_name')
        extract_tags = pattern_config.get('extract_tags', [])

        if not pattern_str or not new_field_name:
            logger.warning(f"Invalid expansion_pattern config: {pattern_config}")
            return metric

        try:
            pattern = re.compile(pattern_str)
        except re.error as e:
            logger.error(f"Invalid regex pattern '{pattern_str}': {e}")
            return metric

        # Process each field in the metric
        new_fields = {}
        updated_tags = metric.get('tags', {}).copy()

        for field_name, field_value in metric.get('fields', {}).items():
            match = pattern.match(field_name)
            if match:
                # Pattern matched - transform the field
                new_fields[new_field_name] = field_value

                # Extract tags from capture groups
                for tag_config in extract_tags:
                    tag_name = tag_config.get('name')
                    group_num = tag_config.get('group')

                    if tag_name and group_num is not None:
                        try:
                            tag_value = match.group(group_num)
                            updated_tags[tag_name] = str(tag_value)
                        except IndexError:
                            logger.warning(
                                f"Regex group {group_num} not found in pattern '{pattern_str}' "
                                f"for field '{field_name}'"
                            )
            else:
                # Pattern didn't match - keep original field name
                new_fields[field_name] = field_value

        # Update metric with new fields and tags
        metric['fields'] = new_fields
        metric['tags'] = updated_tags

        return metric


class Formatter(MetricsPipeline):

    def process_method(self, metrics):
        formats = load_yaml_file(self.config["formats_filepath"])
        self.formats_compiled = precompile_regex_keys(formats)
        self.combine_formats = self.config.get("combine_formats", False)

        metrics = self.format_metrics(metrics, formats)

        return metrics

    def format_metrics(self, metrics, formats):
        for metric in metrics:
            check_metric_fields_length(metric)

            metric_id = get_metric_id(metric)
            format = build_metric_format(
                formats, self.formats_compiled, metric_id, combine=self.combine_formats
            )

            if not format:
                logging.debug(
                    f"No format found for metric: {metric_id}",
                    extra={"metric": metric},
                )
                continue

            field_key = next(iter(metric["fields"]))  # There is only one field
            field_value = metric["fields"][field_key]

            if format.get("type") == "float":
                metric["fields"][field_key] = float(field_value)
            elif format.get("type") == "str":
                metric["fields"][field_key] = str(field_value)
            else:
                logging.debug(
                    f"Metric:{field_value} - Type not specified in metric format, defaulting to str",
                    extra={"metric": metric},
                )
                metric["fields"][field_key] = str(field_value)

            # Update tags if format contains tags
            if "tags" in format:
                metric["tags"] = deep_merge(metric["tags"], format["tags"])

        return metrics


class PropertyMapper(MetricsPipeline):
    def __init__(self, config=None):
        super().__init__(config)
        self.property_mapping = self.load_property_mapping()

    def load_property_mapping(self):
        # Load the property mapping only once during initialization
        return load_yaml_file(self.config["property_mapping_filepath"])

    def process_method(self, metrics):
        # Directly use the loaded property mapping
        return self.map_metric_properties(metrics)

    def map_metric_properties(self, metrics):
        # Initialize an empty list to store the updated metrics
        updated_metrics = []

        for metric in metrics:
            new_metric = {}
            for property, values in metric.items():
                if property in self.property_mapping:
                    # Map each property using the preloaded mapping
                    if isinstance(values, (dict, list, tuple)):
                        new_values = {
                            self.property_mapping[property].get(p, p): values[p]
                            for p in values
                        }
                    elif isinstance(values, str):
                        new_values = self.property_mapping[property].get(values, values)
                    new_metric[property] = new_values
                else:
                    new_metric[property] = values
            updated_metrics.append(new_metric)

        return updated_metrics


class OutlierRemover(MetricsPipeline):

    def __init__(self, config=None) -> None:
        super().__init__(config=config)

    def process_method(self, metrics):
        boundaries = load_yaml_file(self.config["boundaries_filepath"])
        self.boundaries_compiled = precompile_regex_keys(boundaries)
        metrics = self.remove_outliers(metrics, boundaries)
        return metrics

    def remove_outliers(self, metrics, boundaries):
        metrics_filtered = []
        metrics_removed = []
        for metric in metrics:
            check_metric_fields_length(metric)
            metric_id = get_metric_id(metric)
            metric_boundaries = build_metric_format(
                formats=boundaries,
                formats_compiled=self.boundaries_compiled,
                metric_id=metric_id,
                combine=self.config.get("combine_boundaries", False),
            )

            if not metric_boundaries:
                logging.debug(
                    f"No boundary found for metric: {metric_id}",
                    extra={"metric": metric},
                )
                continue

            field_key = next(iter(metric["fields"]))  # There is only one field
            field_value = metric["fields"][field_key]

            if metric_boundaries is None:
                metrics_filtered.append(metric)
                continue

            if isinstance(field_value, str):
                metrics_filtered.append(metric)
                continue

            try:
                if (
                    "max" in metric_boundaries
                    and field_value > metric_boundaries["max"]
                ):
                    metrics_removed.append(metric)
                    self.metrics_filtered.labels(
                        agent="metrics_processor",
                        pipeline=self.__class__.__name__,
                        id=field,
                        reason="Value excceeded max",
                    ).inc()
                    continue
            except KeyError:
                pass

            try:
                if (
                    "min" in metric_boundaries
                    and field_value < metric_boundaries["min"]
                ):
                    metrics_removed.append(metric)
                    self.metrics_filtered.labels(
                        agent="metrics_processor",
                        pipeline=self.__class__.__name__,
                        id=field,
                        reason="Value below min",
                    ).inc()
                    continue
            except KeyError:
                pass

            metrics_filtered.append(metric)

        number_of_outliers_removed = len(metrics_removed)

        logger.debug(
            f"Removed {number_of_outliers_removed} metrics: {shorten_data(str(metrics_removed))}"
        )
        return metrics_filtered

    # def remove_outliers(self, metrics, boundaries):
    #     metrics_filtered = []
    #     metrics_removed = []
    #     for metric in metrics:
    #         for field in metric["fields"]:
    #             boundary = boundaries.get(field)
    #             if boundary is None:
    #                 metrics_filtered.append(metric)
    #                 continue

    #             value = metric["fields"][field]
    #             if isinstance(value, str):
    #                 metrics_filtered.append(metric)
    #                 continue

    #             try:
    #                 if "max" in boundary and value > boundary["max"]:
    #                     metrics_removed.append(metric)
    #                     self.metrics_filtered.labels(
    #                         agent="metrics_processor",
    #                         pipeline=self.__class__.__name__,
    #                         id=field,
    #                         reason="Value excceeded max",
    #                     ).inc()
    #                     continue
    #             except KeyError:
    #                 pass

    #             try:
    #                 if "min" in boundary and value < boundary["min"]:
    #                     metrics_removed.append(metric)
    #                     self.metrics_filtered.labels(
    #                         agent="metrics_processor",
    #                         pipeline=self.__class__.__name__,
    #                         id=field,
    #                         reason="Value below min",
    #                     ).inc()
    #                     continue
    #             except KeyError:
    #                 pass

    #             metrics_filtered.append(metric)

    #     number_of_outliers_removed = len(metrics_removed)

    #     logger.debug(
    #         f"Removed {number_of_outliers_removed} metrics: {shorten_data(str(metrics_removed))}"
    #     )
    #     return metrics_filtered


class BinaryOperations(MetricsPipeline):

    config_filepath_key = "binary_operations_filepath"

    def process_method(self, metrics):
        operation_list = load_yaml_file(self.config[self.config_filepath_key])
        metrics = self.operations(metrics, operation_list)
        return metrics

    def operations(self, metrics, operation_list):
        for operation in operation_list:
            operation = operation_list[operation]
            op = operation["operation"]
            operands = operation["operands"]
            operands_metrics = []
            for metric in metrics:
                for field in metric["fields"]:
                    if field in operands:
                        operands_metrics.append(metric)
            operands_value = [next(operand["fields"]) for operand in operands_metrics]
            operands_time = [operand["time"] for operand in operands_metrics]
            time = None
            try:
                if op == "add":
                    result = sum(operands_value)
                elif op == "subtract":
                    result = operands_value[0] - operands_value[1]
                elif op == "multiply":
                    result = operands_value[0] * operands_value[1]
                elif op == "divide":
                    if operands_value[1] == 0:
                        raise ValueError("Division by zero is not allowed")
                    result = operands_value[0] / operands_value[1]
                elif op == "max":
                    result = max(operands_value)
                elif op == "min":
                    result = min(operands_value)
                else:
                    raise ValueError("Invalid operation")

                try:
                    time = max(operands_time)
                except ValueError:
                    time = datetime.now()

                new_metric = {
                    "measurement": operands[0]["measurement"],
                    "fields": {operation["result"]: result},
                    "tags": operands[0].get("tags", {}),
                    "time": time,
                }
                metrics.append(new_metric)

            except ValueError as e:
                logging.error(f"Error in binary operation: {e}")
                continue

        return metrics


class PropertyConstructor(MetricsPipeline):

    def __init__(self, config=None) -> None:
        super().__init__(config=config)
        self.property_recipes = self.config.get("property_recipes")
        self.property_group = self.config.get("property_group")

    def process_method(self, metrics):

        def build_properties(recipes, metric):

            # Check if metric has more than one field
            if len(metric["fields"]) > 1:
                message = "Metric has more than one field, cannot build properties. This is resolved by applying FieldExpander before PropertyConstructor"
                logger.error(message)
                raise ValueError(message)

            new_fields = {}

            for property, structure in recipes.items():
                property_fields = structure.split("/")
                try:
                    property_value = []
                    for field in property_fields:
                        if field == "field":
                            property_value.append(next(iter(metric["fields"])))
                        else:
                            property_value.append(metric[field])
                    property_value = "/".join(property_value)
                except KeyError:
                    message = f"Property field not found in metric: {property_fields}"
                    logger.error(message)
                    raise KeyError(message)

                new_fields[property] = property_value

            if new_fields:
                return new_fields
            else:
                return None

        if not self.property_recipes:
            logger.warning(
                f"No property recipes specified for {self.__class__.__name__}. Continuing without modification"
            )
            return metrics

        for i, metric in enumerate(metrics):

            new_properties = build_properties(self.property_recipes, metric)
            # Note this will only work for creating property groups that are dictionaries
            # Other types are not considered
            if new_properties:
                if self.property_group:
                    # Catch the case where the property group is None
                    property_group = metric.get(self.property_group) or {}
                    metrics[i][self.property_group] = property_group | new_properties
                else:
                    metrics[i] = metric | new_properties

        return metrics
