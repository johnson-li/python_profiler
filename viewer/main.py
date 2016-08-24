import calendar
import datetime
import gzip
import inspect
import json
import os
import subprocess
import time
import uuid

from flask import Flask, request, send_file, send_from_directory, render_template, redirect, url_for

import stack_profiler_viewer

app = Flask(__name__)


@app.route("/")
def hello():
    return redirect(url_for('help_page'))


@app.route("/help")
def help_page():
    return render_template('help.html')


@app.route('/static/<path:path>')
def send_js(path):
    return send_from_directory('static', path)


@app.route('/generic_profiler/detail', methods=['GET'])
def generic_profiler():
    date = datetime.datetime.fromtimestamp(stack_profiler_viewer.valid_date(request.args.get('date', int(time.time()))))
    repo = request.args.get('repo', 'default')
    host = request.args.get('host', 'default')
    line_num = int(request.args.get('line', 0))
    path = '/logdir/{}/{}{:02d}{:02d}/{}.perftree/{}.log'.format(date.year, date.year, date.month, date.day, repo,
                                                                      host)
    if os.path.isfile(path):
        with open(path) as f:
            for i, line in enumerate(f):
                if i == line_num:
                    _, json_str = line.split(': ', 1)
                    j = json.loads(json_str)
                    parameter = j.pop('parameter', None)
                    return render_template('tree.html', json_str=json.dumps(j), parameter=json.dumps(parameter))
    else:
        path += '.gz'
        if os.path.isfile(path):
            with gzip.open(path) as f:
                for i, line in enumerate(f):
                    if i == line_num:
                        _, json_str = line.split(': ', 1)
                        j = json.loads(json_str)
                        parameter = j.pop('parameter', None)
                        return render_template('tree.html', json_str=json.dumps(j), parameter=json.dumps(parameter))

        else:
            return 'No data'
    return 'last'


@app.route("/generic_profiler/thumbnail", methods=['GET'])
def generic_profiler_thumbnail():
    date = datetime.datetime.fromtimestamp(stack_profiler_viewer.valid_date(request.args.get('date', int(time.time()))))
    repo = request.args.get('repo', 'default')
    hosts = request.args.get('hosts', None)
    hosts = hosts.split('+') if hosts else []
    log_dir = '/logdir/{}/{}{:02d}{:02d}/{}.perftree'.format(date.year, date.year, date.month, date.day, repo)
    log_paths = ['{}/{}.log'.format(log_dir, host) for host in hosts] if hosts else \
        ['{}/{}'.format(log_dir, file_name) for file_name in (os.listdir(log_dir) if os.path.isdir(log_dir) else [])]
    if not hosts:
        hosts = [path.split('/')[-1].split('.')[0] for path in log_paths]
    data_list = []

    def handle_file(host, ft):
        for line_num, line in enumerate(ft):
            case = {'time': line[:15], 'line_num': line_num, 'path': log_path, 'host': host}
            line = line[16:]
            _, line = line.split(' ', 1)
            _, content = line.split(': ', 1)
            try:
                content_json = json.loads(content)
            except Exception as e:
                print e
                continue
            content_json.pop('parameter', None)
            case['title'] = content_json.keys()[0]
            case['link'] = '/generic_profiler/detail?repo={}&date={}&line={}&host={}'. \
                format(repo, date.date(), line_num, host)
            data_list.append(case)

    for host, log_path in zip(hosts, log_paths):
        if os.path.isfile(log_path):
            if log_path.endswith('.gz'):
                with gzip.open(log_path) as f:
                    handle_file(host, f)
            else:
                with open(log_path) as f:
                    handle_file(host, f)
        else:
            log_path += '.gz'
            if os.path.isfile(log_path):
                with gzip.open(log_path, 'r') as f:
                    handle_file(host, f)
    if data_list:
        return render_template('thumbnail.html', cases=sorted(data_list, key=lambda x: x['time'], reverse=True))
    else:
        return 'No data'


def get_stack_profiler_path(start, end, repo):
    date_list = [datetime.datetime.fromtimestamp(end)]
    date_now = date_list[0].date()
    for i in range(1, 4):
        date_pre = date_now - datetime.timedelta(days=1)
        ts_now = calendar.timegm(date_now.timetuple())
        ts_pre = calendar.timegm(date_pre.timetuple())
        if ts_pre <= start <= ts_now:
            date_list.append(date_pre)
        else:
            break
        date_now = date_pre

    print date_list
    return ['/logdir/{}/{}{:02d}{:02d}/{}.stack'.format(date.year, date.year, date.month, date.day, repo) for
            date in set(date_list)]


@app.route("/stack_profiler", methods=['GET'])
def stack_profiler():
    start = stack_profiler_viewer.valid_date(request.args.get('start', int(time.time() - 3600)))
    end = stack_profiler_viewer.valid_date(request.args.get('end', int(time.time())))
    repo = request.args.get('repo', 'default')
    show_others = request.args.get('show_others', False)
    formatter = stack_profiler_viewer.FlamegraphFormatter()
    output_file = '/tmp/stack_profiler/' + str(uuid.uuid4()) + '.fold'
    output_file_svg = '/tmp/stack_profiler/' + str(uuid.uuid4()) + '.svg'
    stack_profiler_viewer.fold_data(get_stack_profiler_path(start, end, repo), output_file, start, end, formatter,
                                    show_others)
    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    subprocess.call(['{}/flamegraph.pl'.format(path), output_file], stdout=open(output_file_svg, 'w'))
    resp = send_file(output_file_svg, as_attachment=False, attachment_filename='stack_profiler.svg')
    os.remove(output_file)
    os.remove(output_file_svg)
    return resp


if __name__ == "__main__":
    app.run(host='0.0.0.0')
