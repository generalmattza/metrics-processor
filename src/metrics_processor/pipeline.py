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

    processing_duration_seconds = Histogram(
        "metrics_processor_pipeline_processing_duration_seconds",
        "Per-metric processing duration within a pipeline stage (seconds)",
        ["pipeline"],
    )

    metrics_processed_total = Counter(
        "metrics_processor_pipeline_metrics_processed_total",
        "Number of metrics processed by a pipeline stage",
        ["pipeline"],
    )

    metrics_filtered_total = Counter(
        "metrics_processor_pipeline_metrics_filtered_total",
        "Number of metrics filtered out by a pipeline stage",
        ["pipeline", "id", "reason"],
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
            self.processing_duration_seconds.labels(
                pipeline=self.__class__.__name__
            ).observe((end_time - start_time) / number_of_metrics)
            self.metrics_processed_total.labels(
                pipeline=self.__class__.__name__
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
        self.metrics_filtered_total.labels(
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
    Transform field names via regex-based field_transform config.

    Renames fields by pattern match (e.g. stripping namespace prefixes)
    and optionally extracts portions of the field name into tags or context.

    Example:
        Input metric:
            fields: {"IO_Internal_AI_PV.DUTY_CYCLE_1": 45.2}
            field_transform:
                pattern: "^IO_Internal_AI_PV\\.(DUTY_CYCLE_\\d+)$"
                field_name: "$1"

        Output metric:
            fields: {"DUTY_CYCLE_1": 45.2}
    """

    def __init__(self, config=None) -> None:
        super().__init__(config=config)
        self._pattern_cache = {}  # Cache compiled regex patterns for performance

    def process_method(self, metrics):
        result = []
        for metric in metrics:
            field_transform = metric.get('field_transform')
            if field_transform:
                metric = self._apply_field_transform(metric, field_transform)
                metric.pop('field_transform', None)
            result.append(metric)
        return result

    def _apply_field_transform(self, metric: dict, pattern_config: dict) -> dict:
        """
        Apply the field transform to rename fields and optionally extract tags.

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
            logger.warning(f"Invalid field_transform config: {pattern_config}")
            return metric

        # Use cached compiled pattern for performance (avoids re-compiling on every metric)
        if pattern_str not in self._pattern_cache:
            try:
                self._pattern_cache[pattern_str] = re.compile(pattern_str)
            except re.error as e:
                logger.error(f"Invalid regex pattern '{pattern_str}': {e}")
                return metric

        pattern = self._pattern_cache[pattern_str]

        # Process each field in the metric
        new_fields = {}
        updated_tags = metric.get('tags', {}).copy()
        updated_context = metric.get('context', {}).copy()

        for field_name, field_value in metric.get('fields', {}).items():
            match = pattern.match(field_name)
            if match:
                # Pattern matched - transform the field
                resolved_field_name = new_field_name
                if '$' in new_field_name:
                    def replace_group_ref(m):
                        group_num = int(m.group(1))
                        try:
                            return match.group(group_num)
                        except IndexError:
                            logger.warning(
                                f"Regex group {group_num} not found in pattern '{pattern_str}' "
                                f"for field_name reference '${group_num}'"
                            )
                            return m.group(0)
                    resolved_field_name = re.sub(r'\$(\d+)', replace_group_ref, new_field_name)
                new_fields[resolved_field_name] = field_value

                # Extract tags from capture groups
                for tag_config in extract_tags:
                    tag_name = tag_config.get('name')
                    group_num = tag_config.get('group')
                    target = tag_config.get('target', 'tags')

                    if tag_name and group_num is not None:
                        try:
                            tag_value = match.group(group_num)
                            if target == 'context':
                                updated_context[tag_name] = str(tag_value)
                            else:
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
        if updated_context:
            metric['context'] = updated_context

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
                    self.metrics_filtered_total.labels(
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
                    self.metrics_filtered_total.labels(
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


class TagMapper(MetricsPipeline):
    """
    Map extracted tags to additional tags using TOML configuration files.

    This pipeline stage looks up tag values in TOML mapping files and adds
    additional tags based on those mappings. It's designed to work after
    FieldToTagMapper to enrich metrics with descriptive labels and groups.

    Example:
        Input metric (after FieldToTagMapper):
            fields: {"TT_LHT": 150.5}
            tags: {"channel": "1", ...}
            tag_mapping_config: {
                "mapping_file": "config/channel_mappings.toml",
                "source_tag": "channel"  # Optional - will be inferred
            }

        TOML file (config/channel_mappings.toml):
            [channel.1]
            group = "Outer drum bottom heater TCs"
            label = "Bottom Outer Die, 90° - A1"

            [channel.2]
            group = "Outer die bottom heater TCs"
            label = "Bottom Outer Die, 45° - A2"

        Output metric:
            fields: {"TT_LHT": 150.5}
            tags: {
                "channel": "1",
                "group": "Outer drum bottom heater TCs",
                "label": "Bottom Outer Die, 90° - A1",
                ...
            }

    Note: Both [channel.1] and ["channel.1"] TOML syntax are supported.
          Nested structures are automatically flattened.
    """

    def __init__(self, config=None) -> None:
        super().__init__(config=config)
        self._mapping_cache = {}  # Cache loaded TOML files for performance
        self._source_tag_cache = {}  # Cache inferred source_tag per mapping file

    def process_method(self, metrics):
        result = []
        for metric in metrics:
            # Check if this metric has tag_mapping_config
            tag_mapping_config = metric.get('tag_mapping_config')
            if tag_mapping_config:
                metric = self._apply_tag_mapping(metric, tag_mapping_config)
                # Remove tag_mapping_config from metric after applying
                metric.pop('tag_mapping_config', None)
            result.append(metric)
        return result

    def _apply_tag_mapping(self, metric: dict, mapping_config: dict) -> dict:
        """
        Apply tag mappings from a TOML file to add additional tags.

        Args:
            metric: The metric dict with fields, tags, measurement, time, etc.
            mapping_config: Dict with keys:
                - mapping_file: Path to TOML file with mappings
                - source_tag: (Optional) Tag name to use as lookup key (e.g., "channel")
                              If not specified, will be inferred from TOML file structure

        Returns:
            Modified metric dict with additional tags from the mapping file
        """
        mapping_file = mapping_config.get('mapping_file')
        source_tag = mapping_config.get('source_tag')

        if not mapping_file:
            logger.warning(f"Invalid tag_mapping_config: {mapping_config}")
            return metric

        # Load the mapping file (with caching)
        mappings = self._load_mapping_file(mapping_file)
        if mappings is None:
            return metric

        # Get metric tags
        tags = metric.get('tags', {})

        # If source_tag not specified, infer it from TOML structure (with caching)
        if not source_tag:
            # Check cache first
            if mapping_file in self._source_tag_cache:
                source_tag = self._source_tag_cache[mapping_file]
            else:
                source_tag = self._infer_source_tag(mappings)
                if source_tag:
                    # Cache the inferred source_tag for this mapping file
                    self._source_tag_cache[mapping_file] = source_tag
                else:
                    logger.debug(
                        f"Could not infer source_tag from {mapping_file}."
                    )
                    return metric

        # Get the value of the source tag (check context as fallback)
        source_value = tags.get(source_tag) or metric.get('context', {}).get(source_tag)

        if source_value is None:
            logger.debug(
                f"Source tag '{source_tag}' not found in metric tags. "
                f"Available tags: {list(tags.keys())}"
            )
            return metric

        # Look up the mapping in the TOML file
        # TOML sections are like [channel.1], [channel.2], etc.
        section_key = f"{source_tag}.{source_value}"
        mapped_tags = mappings.get(section_key)

        if mapped_tags is None:
            logger.debug(
                f"No mapping found for '{section_key}' in {mapping_file}"
            )
            return metric

        # Add the mapped tags to the metric
        updated_tags = tags.copy()
        updated_tags.update(mapped_tags)

        metric['tags'] = updated_tags

        logger.debug(
            f"Applied tag mapping for '{section_key}': added {list(mapped_tags.keys())}"
        )

        return metric

    def _infer_source_tag(self, mappings: dict) -> str | None:
        """
        Infer the source_tag from TOML file structure.

        TOML sections like [data_name."IO_Internal_AI_PV.TT_LHT_1"] indicate
        the source tag is "data_name". If all section keys share the same prefix,
        that prefix is the source tag.

        Args:
            mappings: Dict loaded from TOML file

        Returns:
            Inferred source tag name, or None if cannot determine
        """
        # Extract unique tag names from TOML section keys (e.g., "data_name" from "data_name.X")
        tag_names = set()
        for section_key in mappings.keys():
            if '.' in section_key:
                tag_name = section_key.split('.', 1)[0]
                tag_names.add(tag_name)

        if not tag_names:
            logger.warning(f"No tag mappings found in TOML sections")
            return None

        if len(tag_names) == 1:
            source_tag = tag_names.pop()
            logger.debug(f"Inferred source_tag: {source_tag}")
            return source_tag
        else:
            logger.warning(
                f"Multiple tag name prefixes found in TOML: {tag_names}. "
                f"Cannot infer source_tag — specify it explicitly in tag_mapping_config."
            )
            return None

    def _load_mapping_file(self, filepath: str) -> dict | None:
        """
        Load a TOML mapping file, with caching for performance.

        Handles both flat keys (["channel.1"]) and nested structure ([channel.1]).
        Nested structures are automatically flattened to dot notation.

        Args:
            filepath: Path to the TOML file

        Returns:
            Dict with mapping sections (flattened to dot notation), or None if file cannot be loaded
        """
        # Check cache first
        if filepath in self._mapping_cache:
            return self._mapping_cache[filepath]

        # Load the file
        try:
            mappings = load_toml_file(filepath)
            # Flatten nested structure if needed
            flattened = self._flatten_nested_mappings(mappings)
            self._mapping_cache[filepath] = flattened
            logger.info(f"Loaded tag mapping file: {filepath} ({len(flattened)} mappings)")
            return flattened
        except FileNotFoundError:
            logger.error(f"Tag mapping file not found: {filepath}")
            return None
        except Exception as e:
            logger.error(f"Error loading tag mapping file {filepath}: {e}")
            return None

    def _flatten_nested_mappings(self, mappings: dict, parent_key: str = '') -> dict:
        """
        Flatten nested TOML structure to dot notation.

        Converts:
            {"channel": {"1": {"group": "...", "label": "..."}}}
        To:
            {"channel.1": {"group": "...", "label": "..."}}

        Args:
            mappings: Raw dict from TOML file
            parent_key: Parent key for recursion

        Returns:
            Flattened dict with dot notation keys
        """
        flattened = {}

        for key, value in mappings.items():
            new_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, dict):
                # Check if this dict contains mapping values (group, label, etc.)
                # or if it's another level of nesting
                has_mapping_keys = any(k in value for k in ['group', 'label'])
                has_only_dicts = all(isinstance(v, dict) for v in value.values())

                if has_mapping_keys:
                    # This is a leaf node with actual mappings
                    flattened[new_key] = value
                elif has_only_dicts and not has_mapping_keys:
                    # This is a nested structure, recurse
                    flattened.update(self._flatten_nested_mappings(value, new_key))
                else:
                    # Mixed structure - treat as leaf
                    flattened[new_key] = value
            else:
                # Scalar value - treat as leaf
                flattened[new_key] = value

        return flattened
