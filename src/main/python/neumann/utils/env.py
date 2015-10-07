import os
from neumann.utils.logger import Logger


def exclude_env(env_name):
    """
    Decorator that skips execution of function if certain environment variable is present.

    :param env_name: the name of environment variable to check for.
    """
    def wrapper(f):
        if env_name in os.environ:
            def excluded(*args):
                Logger.warning("Call to function {0}{1} is disabled due to {2} present".format(f.__name__,
                                                                                               args[1:], env_name))
            return excluded
        else:
            return f
    return wrapper
