import copy
import functools
import hashlib
import inspect
import os
import pickle
import shutil
import time
from typing import Callable, Optional

from cloudstorageio.tools.logger import logger


def timer(func) -> Callable:
    """A decorator which prints execution time of the decorated function"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        logger.info("Executed {} in {:.4f} seconds"
              .format(func.__name__, (time.time() - start_time)))
        return result
    return wrapper


def storage_cache_factory(path: str = '/tmp/cache') -> Callable:
    """Factory decorator for modifying the decorated function to cache
       and reuse results in a predefined path on the local storage
    :param path: Path to the local cache location
    :return: Decorator
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            copy_args = copy.deepcopy(list(args))

            all_args = {}
            function_class_type = ''

            # finding name of args and map name to value for having key value pair in the end
            arg_specs = inspect.getfullargspec(func)

            # takes care of default arguments
            if arg_specs.defaults:
                start_idx = len(arg_specs.defaults)
                default_arg_list = list(arg_specs.defaults)
                for i, arg_name in enumerate(arg_specs.args[-start_idx:]):
                    all_args[arg_name] = default_arg_list.pop(0)

            for arg_name in arg_specs.args:
                # avoids instance 'self' param
                if arg_name == 'self':
                    inst = copy_args.pop(0)
                    function_class_type = inst.__class__
                    continue

                if copy_args:
                    all_args[arg_name] = copy_args.pop(0)

            all_args.update(kwargs)
            all_args = ([(k, v) for k, v in all_args.items()])
            all_args = sorted(all_args)
            os.makedirs(path, exist_ok=True)
            hash_ = hashlib.sha256()
            idx = str([func.__module__, func.__name__, function_class_type,  all_args])
            hash_.update(idx.encode('utf8'))
            key = hash_.hexdigest()
            filename = key + '.p'
            if filename in os.listdir(path):
                logger.info('Reading from cache')
                logger.warning("The state of output might be changed after being cached")
                with open(os.path.join(path, filename), 'rb') as f:
                    res = pickle.load(f)
            else:
                res = func(*args, **kwargs)
                # print('Writing to cache')
                with open(os.path.join(path, filename), 'wb') as f:
                    pickle.dump(res, f)
            return res
        return wrapper
    return decorator


if __name__ == "__main__":
    pass

