import argparse
import calendar
import collections
import gzip
import inspect
import json
import os
import re
import subprocess
import time

import dateutil.parser

# import bryo.utils.s3util

SOURCE_FILE_FILTER = re.compile('^/nail/srv/')


class CollectorFormatter(object):
    """
    Abstract class for output formats
    """

    def format(self, stacks):
        raise Exception("not implemented")


class PlopFormatter(CollectorFormatter):
    """
    Formats stack frames for plop.viewer
    """

    def __init__(self, max_stacks=500):
        self.max_stacks = max_stacks

    def format(self, stacks):
        # defaultdict instead of counter for pre-2.7 compatibility
        stack_counts = collections.defaultdict(int)
        for frames, count in stacks:
            stack_counts[tuple([tuple(l) for l in frames])] += count
        stack_counts = dict(sorted(stack_counts.iteritems(),
                                   key=lambda kv: -kv[1])[:self.max_stacks])
        return repr(stack_counts)


class FlamegraphFormatter(CollectorFormatter):
    """
    Creates Flamegraph files
    """

    def format(self, stacks):
        output = ""
        previous = None
        previous_count = 0
        for stack, count in stacks:
            current = self.format_flame(stack)
            if current == previous:
                previous_count += count
            else:
                if previous:
                    output += "%s %d\n" % (previous, previous_count)
                previous_count = count
                previous = current
        output += "%s %d\n" % (previous, previous_count)
        return output

    @staticmethod
    def format_flame(stack):
        funcs = map("{0[2]} ({0[0]}:{0[1]})".format, reversed(stack))
        return ";".join(funcs)


def handle_file(f, start_ts, end_ts, stacks, show_others):
    for line in f:
        # some old version of log does not have count field, so a check is needed
        index = line.find(': ')
        if index > 0:
            line = line[index + 2:]
        index1 = line.index(' ')
        index2 = line.rfind('&&&')
        ts = int(line[:index1])
        count = int(line[index2 + 3:]) if index2 > 0 else 1
        if start_ts <= ts <= end_ts:
            stack = json.loads(line[index1 + 1:index2] if index2 > 0 else line[index1 + 1:])
            back_index = next((i for i, v in enumerate(reversed(stack)) if SOURCE_FILE_FILTER.match(v[0])), -1)
            front_index = next((i for i, v in enumerate(stack) if SOURCE_FILE_FILTER.match(v[0])), -1)
            if back_index >= 0:
                stack = stack[:-back_index] if show_others else stack[front_index:-back_index]
                if stack[0][0] == '/nail/srv/suso/utils/stack_profiler.py':
                    continue
                stacks.append((stack, count))


def get_stacks(path, start_ts, end_ts, show_others):
    stacks = []
    if os.path.isdir(path):
        file_names = [path + '/' + i for i in os.listdir(path)]
    else:
        file_names = [path]
    for file_name in file_names:
        if file_name.endswith('.gz'):
            with gzip.open(file_name) as f:
                handle_file(f, start_ts, end_ts, stacks, show_others)
        else:
            with open(file_name) as f:
                handle_file(f, start_ts, end_ts, stacks, show_others)
    return stacks


def fold_data(input_paths, output_path, start_ts, end_ts, formatter, show_others):
    stack_list = []
    for input_path in input_paths:
        stack_list += get_stacks(input_path, start_ts, end_ts, show_others)
    data = formatter.format(stack_list)

    if output_path:
        f = open(output_path, 'w')
        f.write(data)


def get_dir(file_name):
    directory = '/tmp/stack-profiler'
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory + '/' + file_name


def valid_date(val):
    if isinstance(val, int) or val.isdigit():
        return int(val)
    return calendar.timegm(dateutil.parser.parse(val).utctimetuple())


def main():
    parser = argparse.ArgumentParser(description='', prog='python generate_stack_profiler_data',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--format", "-f", help="Output format", choices=["plop", "flamegraph"], default="flamegraph")
    parser.add_argument("--mode", help="Interval timer mode to use, see `man 2 setitimer`",
                        choices=["prof", "real", "virtual"], default="prof")
    parser.add_argument("--output", help="data output file name", default=get_dir("stack_profiler.folded"))
    parser.add_argument("--output-svg", help="svg output file name, works with -s tag",
                        default=get_dir("stack_profiler.svg"))
    parser.add_argument("--input", help="input file name", default="/nail/logs/emma-stack-profiler.sample")
    parser.add_argument("--start", '-S', help="start timestamp, or time str that can be parsed by dateutil.parser",
                        default=None, type=valid_date)
    parser.add_argument("--end", '-E', help="end timestamp, or time str that can be parsed by dateutil.parser",
                        default=long(time.time()), type=valid_date)
    parser.add_argument("--upload", "-u", action='store_true', help="upload svg output data to s3, works with -s tag")
    parser.add_argument("--delete", "-d", action='store_true', help="delete intermediate files")
    parser.add_argument("--svg", "-s", action='store_true',
                        help="generate flame graph svg if the format is set as flamegraph")
    args = parser.parse_args()

    if args.format == "plop":
        formatter = PlopFormatter()
    else:
        formatter = FlamegraphFormatter()
    fold_data([args.input], args.output, args.start, args.end, formatter, False)
    if args.svg:
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        subprocess.call(['{}/flamegraph.pl'.format(path), args.output], stdout=open(args.output_svg, 'w'))
    if args.upload and args.svg:
        pass
        # url = bryo.utils.s3util.put('stack_profiler/stack:{}-{}.svg'.format(args.start if args.start else 0, args.end),
        #                             open(args.output_svg), False)
        # print 'uploaded to url:', url
    if args.delete:
        os.remove(args.output)
        if args.svg:
            os.remove(args.output_svg)


if __name__ == '__main__':
    main()
