import json
import logging
import signal
import sys
import time
from signal import ITIMER_REAL, ITIMER_VIRTUAL, ITIMER_PROF

import gevent
import gevent.monkey
import six

data_logger = logging.getLogger('stack_profiler_data')
logger = logging.getLogger(__name__)


class Collector(object):
    MODES = {
        'prof': (ITIMER_PROF, signal.SIGPROF),
        'virtual': (ITIMER_VIRTUAL, signal.SIGVTALRM),
        'real': (ITIMER_REAL, signal.SIGALRM),
    }

    def __init__(self, interval, flush_period_time, mode):
        self.interval = interval
        self.flush_period = int(flush_period_time / interval)
        self.mode = mode
        self.stack_records = {}
        self.perf_counts = 0
        self.handling = False
        assert mode in Collector.MODES
        timer, sig = Collector.MODES[self.mode]
        signal.signal(sig, self.handler)
        signal.siginterrupt(sig, False)
        self.stopping = False
        self.stopped = True

    def start(self):
        self.stopping = False
        self.stopped = False
        self.perf_counts = 0
        self.stack_records.clear()
        timer, sig = Collector.MODES[self.mode]
        signal.setitimer(timer, self.interval, self.interval)

    def stop(self):
        self.stopping = True
        self.wait()

    def wait(self):
        while not self.stopped:
            pass  # need busy wait; ITIMER_PROF doesn't proceed while sleeping

    @staticmethod
    def log(stack_counts):
        t = int(time.time())
        for frames, count in stack_counts.iteritems():
            line = json.dumps(frames)
            data_logger.info('{} {}&&&{}'.format(t, line, count))

    def flush(self):
        Collector.log(self.stack_records)
        self.stack_records.clear()

    def handler(self, sig, current_frame):
        # If the previous handler had not finished, skip this interruption
        if self.handling:
            # logger.warn("interrupt inside previous interrupt, withdraw")
            return
        self.handling = True
        if self.stopping:
            signal.setitimer(Collector.MODES[self.mode][0], 0, 0)
            self.stopped = True
            return
        # Module thread is patched by gevent, so we should get the original func
        current_tid = gevent.monkey.get_original('thread', 'get_ident')()
        for tid, frame in six.iteritems(sys._current_frames()):
            frames = []
            while frame is not None:
                code = frame.f_code
                frames.append((code.co_filename, code.co_firstlineno, code.co_name))
                frame = frame.f_back
            frames = tuple(frames[1:] if tid == current_tid else frames)
            self.stack_records[frames] = self.stack_records.get(frames, 0) + 1
        self.perf_counts += 1
        if self.perf_counts >= self.flush_period:
            self.flush()
            self.perf_counts = 0
        self.handling = False


class StackProfiler(object):
    def __init__(self, interval=0.01, flush_period_time=60, mode='real'):
        self.collector = Collector(interval, flush_period_time, mode)

    def start(self):
        logger.info('start stack profiler')
        self.collector.start()

    def stop(self):
        self.collector.stop()
