import functools
import importlib
import inspect
import json
import logging
import pprint
import sys
import threading
import time

thread_local = None

logger = logging.getLogger(__name__)
data_logger = logging.getLogger('generic_profiler_data')

WRAPPED = {None, __name__}
ENABLE_CALLER_FRAME_LOG = True
FUNC_NAME_BLACK_LIST = {}


def get_context():
    global thread_local
    if not thread_local:
        thread_local = threading.local()
    return thread_local


def clear():
    ctx = get_context()
    if not ctx:
        return
    if hasattr(ctx, 'performance_tree_root'):
        delattr(ctx, 'performance_tree_root')
    if hasattr(ctx, 'performance_tree'):
        delattr(ctx, 'performance_tree')
    if hasattr(ctx, 'parameter'):
        delattr(ctx, 'parameter')


def get_caller_info():
    frame = inspect.currentframe().f_back.f_back
    file_name = frame.f_code.co_filename
    line_num = frame.f_lineno
    return file_name, line_num


class PerformanceTree(object):
    def __init__(self, parent, func, location, start_ts):
        self.parent = parent
        self.children = []
        self.func = func
        self.location = location
        self.start_ts = start_ts
        self.end_ts = 0
        if parent:
            parent.children.append(self)

    def finish(self, end_ts):
        self.end_ts = end_ts


def init_performance_tree(func, location, start_ts, parameter={}):
    ctx = get_context()
    tree = PerformanceTree(None, func, location, start_ts)
    ctx.performance_tree_root = tree
    ctx.performance_tree = tree
    ctx.parameter = parameter
    return tree


def get_performance_tree_node(func, location, start_ts, para_args=None, para_kwargs=None):
    ctx = get_context()
    if hasattr(ctx, 'performance_tree_root'):
        node = PerformanceTree(ctx.performance_tree, func, location, start_ts)
        ctx.performance_tree = node
        return node
    return init_performance_tree(func, location, start_ts, parameter={'args': para_args, 'kwargs': para_kwargs})


def get_total_time():
    ctx = get_context()
    if ctx is not None and hasattr(ctx, 'performance_tree_root'):
        node = ctx.performance_tree_root
        return node.end_ts - node.start_ts
    return -1


PERCENTAGE_THRESHOLD = 2


def log_performance_tree(threshold=3):
    ctx = get_context()
    if not ctx or not hasattr(ctx, 'performance_tree_root'):
        return
    root = ctx.performance_tree_root
    all_time = root.end_ts - root.start_ts
    if all_time < threshold:
        return

    def get_node_info(func, location, time_elapsed, percentage):
        return '[{:5.1f}%,{:5.3f}s] {}<{}>'.format(percentage, time_elapsed, func, location)

    def search(node, output_parent):
        pre_child = None
        output_children = []
        last_ts = node.start_ts
        for child in node.children:
            time_elapsed = child.start_ts - last_ts
            percentage = time_elapsed * 100 / all_time
            output_pre = get_node_info('<interval>',
                                       'from {} to {}'.format(pre_child.location if pre_child else 'start',
                                                              child.location),
                                       time_elapsed, percentage)
            output_child = {}
            last_ts = search(child, output_child)
            pre_child = child
            output_children.append(output_pre)
            output_children.append(output_child)

        if node.children:
            last_ts = node.children[-1].end_ts
            time_elapsed = node.end_ts - last_ts
            percentage = time_elapsed * 100 / all_time if all_time else 0
            output_children.append(
                get_node_info('<interval>', 'from {} to end'.format(node.children[-1].location),
                              time_elapsed, percentage))

        time_elapsed = node.end_ts - node.start_ts
        percentage = time_elapsed * 100 / all_time if all_time else 0
        output_node = {get_node_info(node.func, node.location, time_elapsed, percentage): output_children}
        output_parent.update(output_node)
        return node.end_ts

    parameter = ctx.parameter
    logger.warn('{} costs {}s, {}'.format(root.func, root.end_ts - root.start_ts, pprint.pformat(parameter)))
    output_root = {'parameter': ctx.parameter}
    search(root, output_root)
    data_logger.info(json.dumps(output_root))


def wraps(func, **kwargs):
    f = functools.wraps(func, **kwargs)
    if hasattr(func, 'im_class'):
        f.im_class = func.im_class
    return f


def should_patch(func_name):
    return (not func_name.startswith('__')) and (func_name not in FUNC_NAME_BLACK_LIST)


class GenericProfiler(object):
    def __init__(self, profiler_cfg):
        self.profiler_cfg = profiler_cfg
        ctx = get_context()

    def wrapper(self, func):
        @wraps(func)
        def wrap(*args, **kwargs):
            ctx = get_context()
            if ctx is not None:
                # wrap everything except real func call in the try statement
                try:
                    caller_file = ''
                    caller_line = ''
                    if ENABLE_CALLER_FRAME_LOG:
                        caller_file, caller_line = get_caller_info()
                    func_name = '{}.{}.{}'.format(func.im_class.__module__, func.im_class.__name__, func.__name__) \
                        if hasattr(func, 'im_class') else '{}.{}'.format(func.__module__, func.__name__)
                    start_ts = time.time()
                    node = get_performance_tree_node(func_name, '{}:{}'.format(caller_file, caller_line), start_ts,
                                                     para_args=args, para_kwargs=kwargs)
                except Exception as e:
                    logger.error(e)

                # call the real func
                res = func(*args, **kwargs)

                try:
                    end_ts = time.time()
                    node.finish(end_ts)
                    # if the node is root, print performance tree
                    if node.parent is not None:
                        ctx.performance_tree = ctx.performance_tree.parent
                except Exception as e:
                    logger.error(e)
                return res
            else:
                logger.warn('ctx is None')
                return func(*args, **kwargs)

        return wrap

    def wrap_class(self, clazz, funcs=None):
        if clazz in WRAPPED:
            return
        WRAPPED.add(clazz)
        logger.info('wrap class: ' + clazz.__module__ + '.' + clazz.__name__)
        for para_name in (dir(clazz) if not funcs else funcs):
            if not should_patch(para_name):
                continue
            if not hasattr(clazz, para_name):
                logger.error('there is no field named `{}` in {}'.format(para_name, clazz))
                continue
            para = getattr(clazz, para_name)
            if inspect.ismethod(para):
                if not hasattr(para, 'im_class'):
                    setattr(para, 'im_class', clazz)
                setattr(clazz, para_name, self.wrapper(para))
            elif isinstance(para, staticmethod):
                if not hasattr(para, 'im_class'):
                    setattr(para, 'im_class', clazz)
                setattr(clazz, para_name, staticmethod(self.wrapper(para.__func__)))
            elif isinstance(para, classmethod):
                if not hasattr(para, 'im_class'):
                    setattr(para, 'im_class', clazz)
                setattr(clazz, para_name, classmethod(self.wrapper(para.__func__)))

    def wrap_class_by_name(self, module_str, class_names, funcs=None):
        if not isinstance(class_names, list):
            class_names = [class_names]
        try:
            module = importlib.import_module(module_str)
        except ImportError as e:
            logger.error(e)
            return
        for class_name in class_names:
            if hasattr(module, class_name):
                clazz = getattr(module, class_name)
                self.wrap_class(clazz, funcs)
            else:
                logger.error('module {} has no class named {}'.format(module, class_name))

    def wrap_module(self, module_str, funcs=None):
        if module_str in WRAPPED:
            return
        WRAPPED.add(module_str)

        logger.info('wrap module: ' + module_str)

        # Ignore error that module does not exist
        try:
            module = importlib.import_module(module_str)
        except ImportError as e:
            logger.error(e)
            return
        for para_name in (dir(module) if not funcs else funcs):
            if not should_patch(para_name):
                continue
            if not hasattr(module, para_name):
                logger.error('there is no field named `{}` in {}'.format(para_name, module))
                continue
            para = getattr(module, para_name)
            if inspect.isfunction(para) and para.func_name != '<lambda>':
                setattr(module, para_name, self.wrapper(para))

    def wrap(self):
        if hasattr(self.profiler_cfg, 'MODULES_TO_WRAP'):
            for module in self.profiler_cfg.MODULES_TO_WRAP:
                self.wrap_module(module)
        if hasattr(self.profiler_cfg, 'CLASSES_TO_WRAP'):
            for module_class_tuple in self.profiler_cfg.CLASSES_TO_WRAP:
                self.wrap_class_by_name(*module_class_tuple)
        if hasattr(self.profiler_cfg, 'FUNCTIONS_TO_WRAP'):
            for module in self.profiler_cfg.FUNCTIONS_TO_WRAP.keys():
                self.wrap_module(module, self.profiler_cfg.FUNCTIONS_TO_WRAP.get(module))
        if hasattr(self.profiler_cfg, 'CLASS_FUNCTIONS_TO_WRAP'):
            for module in self.profiler_cfg.CLASS_FUNCTIONS_TO_WRAP.keys():
                for clazz in self.profiler_cfg.CLASS_FUNCTIONS_TO_WRAP.get(module):
                    self.wrap_class_by_name(module, clazz,
                                            self.profiler_cfg.CLASS_FUNCTIONS_TO_WRAP.get(module).get(clazz))
