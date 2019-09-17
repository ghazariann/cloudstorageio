import copy
import functools
import hashlib
import inspect
import os
import pickle
import time
from typing import Callable


def timer(func) -> Callable:
    """A decorator which prints execution time of the decorated function"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print("Executed {} in {:.4f} seconds"
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
            all_args = []
            # finding name of args and map name to value for having key value pair in the end
            arg_specs = inspect.getfullargspec(func)
            for i, arg_name in enumerate(arg_specs.args):
                # print(arg_name)
                if copy_args:
                    all_args.append((arg_name, copy_args.pop(0)))
                    # print(all_args)

            all_args.extend([(k, v) for k, v in kwargs.items()])
            all_args = sorted(all_args)
            os.makedirs(path, exist_ok=True)
            hash_ = hashlib.sha256()
            idx = str([func.__module__, func.__name__, all_args])
            hash_.update(idx.encode('utf8'))
            key = hash_.hexdigest()
            filename = key + '.p'
            if filename in os.listdir(path):
                print('Reading from cache')
                with open(os.path.join(path, filename), 'rb') as f:
                    res = pickle.load(f)
            else:
                res = func(*args, **kwargs)
                print('Writing to cache')
                with open(os.path.join(path, filename), 'wb') as f:
                    pickle.dump(res, f)
            return res
        return wrapper
    return decorator


if __name__ == "__main__":
    # Simple test for storage_cache_factory

    import shutil

    @timer
    @storage_cache_factory()
    def expensive_function():
        time.sleep(2)

    print("First call")
    expensive_function()
    print("Second call")
    expensive_function()
    # Clean the cache
    shutil.rmtree('/tmp/cache')
