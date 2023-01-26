import argparse
import calendar
import datetime
import os
import re
import sys
import time

USE_RICH = False
try:
    import rich
    from rich.console import Console
    from rich.rule import Rule
    from rich.style import Style
    from rich.theme import Theme

    console = Console()

    USE_RICH = True
except ImportError:
    pass

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
        self._printed_divider_before = False

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
            "--http",
            action="store_true",
            help="Only show HTTP requests in tail output.",
        )
        parser.add_argument(
            "--non-http",
            action="store_true",
            help="Only show non-HTTP requests in tail output.",
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

        args = parser.parse_args(argv)
        self.vargs = vars(args)

        self.tail(
            identifier=self.vargs["identifier"],
            colorize=True,
            http=self.vargs["http"],
            non_http=self.vargs["non_http"],
            since=self.vargs["since"],
            filter_pattern=self.vargs["filter"],
            keep_open=not self.vargs["disable_keep_open"],
        )

    # added
    def find_log_group(self, identifier):
        """
        Find the log group for a given identifier.
        """

        paginator = self.logs_client.get_paginator("describe_log_groups")

        log_groups = []
        for page in paginator.paginate():
            log_groups.extend(page["logGroups"])

        print(f"Found {len(log_groups)} log groups")

        candidates = []
        for log_group in log_groups:
            if identifier in log_group["logGroupName"]:
                candidates.append(log_group)

        print(f"Found {len(candidates)} log groups matching {identifier}")
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
            start_time = start_time * 1000
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

    def tail(
        self,
        identifier,
        since,
        filter_pattern,
        limit=10000,
        keep_open=True,
        colorize=True,
        http=False,
        non_http=False,
    ):
        """
        Tail this function's logs.
        if keep_open, do so repeatedly, printing any new logs
        """
        log_group_name = self.find_log_group(identifier)

        try:
            since_stamp = string_to_timestamp(since)
            last_since = since_stamp
            while True:
                new_logs = self.fetch_logs(
                    log_group_name=log_group_name,
                    filter_pattern=filter_pattern,
                    limit=limit,
                    start_time=last_since,
                )

                new_logs = [e for e in new_logs if e["timestamp"] > last_since]

                current_timestamp = 0
                same_timestamp_logs = []
                for log in new_logs:
                    if log["timestamp"] == current_timestamp:
                        same_timestamp_logs.append(log)
                    else:
                        if same_timestamp_logs:
                            if self._printed_divider_before == False:
                                self.print_divider(timestamp=log["timestamp"])
                            self.print_logs(
                                same_timestamp_logs,
                                colorize,
                                http,
                                non_http,
                            )
                        same_timestamp_logs = []
                        current_timestamp = log["timestamp"]

                if not keep_open:
                    break
                if new_logs:
                    last_since = new_logs[-1]["timestamp"]
                time.sleep(1)
        except KeyboardInterrupt:  # pragma: no cover
            # Die gracefully
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(130)

    def on_exit(self):
        pass

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

    def print_divider(self, timestamp=0):
        if USE_RICH:
            divider = Rule(style=Style(color="gray50"), align="left")
            console.print(divider)

        else:
            print("\033[1;30m" + "─" * 80 + "\033[0m")

        self._printed_divider_before = True

    def print_logs(
        self,
        logs,
        colorize=True,
        http=False,
        non_http=False,
    ):
        """
        Parse, filter and print logs to the console.
        """
        has_metadata_logs = False
        for log in logs:
            timestamp = log["timestamp"]
            message = log["message"]
            if self.is_metadata_log(message):
                has_metadata_logs = True
                continue

            timestamp_str = datetime.datetime.fromtimestamp(timestamp / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            if USE_RICH:
                console.print(f"[{timestamp_str}]", message, end="")
            else:
                print(f"\033[1m\033[36m[{timestamp_str}]\033[0m", end=" ")
                print(message, end="")

        if not has_metadata_logs:
            self._printed_divider_before = False


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
