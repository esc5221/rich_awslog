import argparse
import calendar
import datetime
import json
import os
import re
import sys
import time

import rich
from rich.console import Console
from rich.rule import Rule
from rich.style import Style
from rich.theme import Theme
from rich.padding import Padding

console = Console()

import boto3


# ref: durationpy.from_str
def durationpy_from_str(duration):
    _nanosecond_size = 1
    _microsecond_size = 1000 * _nanosecond_size
    _millisecond_size = 1000 * _microsecond_size
    _second_size = 1000 * _millisecond_size
    _minute_size = 60 * _second_size
    _hour_size = 60 * _minute_size
    _day_size = 24 * _hour_size
    _week_size = 7 * _day_size
    _month_size = 30 * _day_size
    _year_size = 365 * _day_size

    units = {
        "ns": _nanosecond_size,
        "us": _microsecond_size,
        "µs": _microsecond_size,
        "μs": _microsecond_size,
        "ms": _millisecond_size,
        "s": _second_size,
        "m": _minute_size,
        "h": _hour_size,
        "d": _day_size,
        "w": _week_size,
        "mm": _month_size,
        "y": _year_size,
    }
    """Parse a duration string to a datetime.timedelta"""

    if duration in ("0", "+0", "-0"):
        return datetime.timedelta()

    pattern = re.compile("([\d\.]+)([a-zµμ]+)")
    total = 0
    sign = -1 if duration[0] == "-" else 1
    matches = pattern.findall(duration)

    if not len(matches):
        raise Exception("Invalid duration {}".format(duration))

    for (value, unit) in matches:
        if unit not in units:
            raise Exception("Unknown unit {} in duration {}".format(unit, duration))
        try:
            total += float(value) * units[unit]
        except:
            raise Exception("Invalid value {} in duration {}".format(value, duration))

    microseconds = total / _microsecond_size
    return datetime.timedelta(microseconds=sign * microseconds)


# ref: zappa.core.utils.string_to_timestamp
def string_to_timestamp(timestring):

    """
    Accepts a str, returns an int timestamp.
    """

    ts = None

    # Uses an extended version of Go's duration string.
    try:
        delta = durationpy_from_str(timestring)
        past = datetime.datetime.now(datetime.timezone.utc) - delta
        ts = calendar.timegm(past.timetuple())
        return ts
    except Exception:
        pass

    if ts:
        return ts
    return 0


# ref: zappa.cli
class ZappaCLI:
    """
    ZappaCLI object is responsible for loading the settings,
    handling the input arguments and executing the calls to the core library.
    """

    # CLI
    vargs = None

    def __init__(self):
        self.logs_client = boto3.client("logs")

        self.last_log_group_color = "gray50"
        self._printed_divider_before = False

        self.config = self.load_config()

    def handle(self, argv=None):
        """
        Main function.
        Parses command, load settings and dispatches accordingly.
        """

        desc = "Tailing AWS CloudWatch Logs"
        parser = argparse.ArgumentParser(description=desc)

        parser.add_argument(
            "identifier",
            type=str,
            help="The identifier of the cloudwatch log group.",
        )

        parser.add_argument(
            "--since",
            type=str,
            default="100000s",
            help="Only show lines since a certain timeframe.",
        )
        parser.add_argument(
            "--filter", type=str, default="", help="Apply a filter pattern to the logs."
        )
        parser.add_argument(
            "--disable-keep-open",
            action="store_true",
            help="Exit after printing the last available log, rather than keeping the log open.",
        )
        parser.add_argument(
            "-e",
            "--exact",
            action="store_true",
            help="Use identifier as an exact match.",
        )
        parser.add_argument(
            "-s",
            "--set",
            action="store_true",
            help="Use identifier as log set name.",
        )

        args = parser.parse_args(argv)
        self.vargs = vars(args)

        self.tail(
            identifier=self.vargs["identifier"],
            use_set=self.vargs["set"],
            since=self.vargs["since"],
            filter_pattern=self.vargs["filter"],
            keep_open=not self.vargs["disable_keep_open"],
            exact=self.vargs["exact"],
        )

    def tail(
        self,
        identifier,
        since,
        filter_pattern,
        limit=10000,
        keep_open=True,
        exact=False,
        use_set=False,
    ):
        """
        Tail this function's logs.
        if keep_open, do so repeatedly, printing any new logs
        """
        if exact and not use_set:
            log_group_name = identifier
        elif not exact and not use_set:
            log_group_name = self.find_log_group(identifier)
        elif use_set:
            self.log_set_name = identifier
            log_group_names = self.get_log_group_names_from_log_set()
        else:
            raise Exception("Invalid combination of arguments.")

        try:
            since_stamp = string_to_timestamp(since) * 1000
            last_since = since_stamp
            while True:
                if not use_set:
                    new_logs = self.fetch_logs(
                        log_group_name=log_group_name,
                        filter_pattern=filter_pattern,
                        limit=limit,
                        start_time=last_since,
                    )
                else:
                    new_logs = []
                    for log_group_name in log_group_names:
                        new_logs_of_group = self.fetch_logs(
                            log_group_name=log_group_name,
                            filter_pattern=filter_pattern,
                            limit=limit,
                            start_time=last_since,
                        )

                        for log in new_logs_of_group:
                            log["log_group_name"] = log_group_name

                        new_logs.extend(new_logs_of_group)

                    new_logs = sorted(new_logs, key=lambda k: k["timestamp"])

                new_logs = [e for e in new_logs if e["timestamp"] > last_since]

                self.print_logs(
                    new_logs,
                )

                last_since = (
                    max([e["timestamp"] for e in new_logs]) if new_logs else last_since
                )

                if not keep_open:
                    break
                # if new_logs:

                time.sleep(1)
        except KeyboardInterrupt:  # pragma: no cover
            # Die gracefully
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(130)

    def on_exit(self):
        pass

    # added
    def find_log_group(self, identifier):
        """
        Find the log group for a given identifier.
        """

        paginator = self.logs_client.get_paginator("describe_log_groups")

        log_groups = []
        for page in paginator.paginate():
            log_groups.extend(page["logGroups"])

        candidates = []
        for log_group in log_groups:
            if identifier in log_group["logGroupName"]:
                candidates.append(log_group)

        for i, candidate in enumerate(candidates):
            print(f"\033[36m[{i}]\033[0m \033[32m{candidate['logGroupName']}\033[0m")

        if len(candidates) == 0:
            raise Exception(f"No log groups found matching {identifier}")
        elif len(candidates) == 1:
            return candidates[0]["logGroupName"]

        choice = input("Which log group would you like to tail? ")
        try:
            choice = int(choice)
        except:
            raise Exception("Invalid choice")

        if choice < 0 or choice >= len(candidates):
            raise Exception("Invalid choice")

        return candidates[choice]["logGroupName"]

    def fetch_log_stream_names(self, log_group_name):
        streams = self.logs_client.describe_log_streams(
            logGroupName=log_group_name, descending=True, orderBy="LastEventTime"
        )

        all_streams = streams["logStreams"]
        log_stream_names = [stream["logStreamName"] for stream in all_streams]
        return log_stream_names

    def fetch_logs(self, log_group_name, filter_pattern="", limit=100000, start_time=0):
        """
        Fetch the CloudWatch logs for a given Lambda name.
        """

        events = []
        response = {}
        while not response or "nextToken" in response:
            all_names = self.fetch_log_stream_names(log_group_name)
            extra_args = {}
            if "nextToken" in response:
                extra_args["nextToken"] = response["nextToken"]

            # Amazon uses millisecond epoch for some reason.
            # Thanks, Jeff.
            end_time = int(time.time()) * 1000

            response = self.logs_client.filter_log_events(
                logGroupName=log_group_name,
                logStreamNames=all_names,
                startTime=start_time,
                endTime=end_time,
                filterPattern=filter_pattern,
                limit=limit,
                interleaved=True,  # Does this actually improve performance?
                **extra_args,
            )

            if response and "events" in response:
                events += response["events"]

        return sorted(events, key=lambda k: k["timestamp"])

    def is_metadata_log(self, message):
        """
        Check if the message is a metadata log.
        """
        if "START RequestId" in message:
            return True
        if "REPORT RequestId" in message:
            return True
        if "END RequestId" in message:
            return True
        return False

    def build_indicator_string(self, color, index, max_index, width=4):
        """
        Build the indicator string for a given index.
        1:
            "[COLOR]    "
        2:
            "  [COLOR]  "
        3:
            "    [COLOR]"
        """
        # "\033[49m" equivalent to "[/]"
        default_color_str = "[on default]"
        color_str = f"[on {color}]"
        indicator_str = (
            default_color_str
            + " " * index * width
            + "[/]"
            + color_str
            + " " * width
            + "[/]"
            + default_color_str
            + (" " * (max_index - index - 1) * width)
            + "[/]"
        )
        return indicator_str

    def print_divider(self, indicator_color="gray50"):
        rule_width = console.width - 8
        divider = "[gray50]" + "─" * rule_width + "[/]"
        if getattr(self, "last_indicator_str", None) is None:
            console.print(f"[on {indicator_color}]" + " " * 2 + "[/]", end="")
        else:
            console.print(
                f"[on {indicator_color}]" + self.last_indicator_str + "[/]", end=""
            )
        console.print(divider)

    def print_logs(
        self,
        logs,
    ):
        """
        Parse, filter and print logs to the console.
        """
        last_timestamp = 0
        for log in logs:
            timestamp = log["timestamp"]
            message = log["message"]

            log_group_name = log.get("log_group_name", None)

            if self.is_metadata_log(message):
                continue

            if last_timestamp < timestamp:
                self.print_divider(self.last_log_group_color)

            timestamp_str = datetime.datetime.fromtimestamp(timestamp / 1000).strftime(
                "%y-%m-%d %H:%M:%S"
            )

            if log_group_name:
                log_group_index, len_log_group = self.get_log_group_index(
                    log_group_name=log_group_name
                )

                log_group_color = (
                    f"color({log_group_index+1})"  # rich.ansi.SGR_STYLE_MAP
                )

                if self.last_log_group_color != log_group_color:
                    self.print_divider(log_group_color)
                    self.last_log_group_color = log_group_color

            # print background as log group color
            for i in range(0, len(message), console.width - 48):
                if log_group_name:
                    indicator_str = self.build_indicator_string(
                        color=log_group_color,
                        index=log_group_index,
                        max_index=len_log_group,
                        width=2,
                    )
                    self.last_indicator_str = indicator_str
                    console.print(
                        f"[on {log_group_color}]" + indicator_str + "[/]", end=""
                    )
                console.print(
                    f"\[{timestamp_str}]",
                    message[i : i + console.width - 48],
                    end="",
                )
                if i + console.width - 48 < len(message):
                    console.print()

            last_timestamp = timestamp

    def load_config(self):
        """
        Load the config file.
        """
        config_path = (
            os.path.dirname(os.path.realpath(__file__)) + "/.awslog_config.json"
        )
        if not os.path.exists(config_path):
            return {}

        with open(config_path, "r") as f:
            return json.load(f)

    def get_log_group_names_from_log_set(self):
        if self.log_set_name not in self.config:
            raise Exception(f"Log set {self.log_set_name} not found in config file")
        log_set = self.config[self.log_set_name]
        log_group_names = [log_group["name"] for log_group in log_set["log_groups"]]

        return log_group_names

    def get_log_group_index(self, log_group_name):
        log_set = self.config[self.log_set_name]
        log_group_names = [log_group["name"] for log_group in log_set["log_groups"]]

        return log_group_names.index(log_group_name), len(log_group_names)


def handle():  # pragma: no cover
    """
    Main program execution handler.
    """
    try:
        if len(sys.argv) == 1:
            file_path = os.path.dirname(os.path.realpath(__file__))
            print("add below alias to ~/.zshrc or ~/.bashrc\n")
            print(f"alias awslog='python3 {file_path}/awslog.py'")
            print()
            print(
                "You can define custom log set in below file and tail merged logs with `awslog <log set name> -s`"
            )
            print(f"{file_path}/.awslog_config.json")
            print()
            return
    except Exception:
        pass

    try:
        cli = ZappaCLI()
        sys.exit(cli.handle())
    except SystemExit as e:  # pragma: no cover
        cli.on_exit()
        sys.exit(e.code)

    except KeyboardInterrupt:  # pragma: no cover
        cli.on_exit()
        sys.exit(130)
    except Exception:
        cli.on_exit()

        print("\n==============\n")
        import traceback

        traceback.print_exc()
        print("\n==============\n")

        sys.exit(-1)


if __name__ == "__main__":  # pragma: no cover
    handle()
