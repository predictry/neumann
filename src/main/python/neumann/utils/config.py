import os
import os.path
import configparser

from neumann.core import errors

PROJECT_BASE = './'
CONFIG_DIR = None
for config_dir in os.path.join(os.curdir, 'config', 'dev'), os.path.join(os.curdir, 'config', 'prod'), '/etc/neumann':
    if os.path.isdir(config_dir) and os.path.exists(config_dir):
        CONFIG_DIR = config_dir
        break

CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.ini')
TASK_CONFIG_FILE = os.path.join(CONFIG_DIR, 'tasks.ini')
LOGGING_CONFIG_FILE = os.path.join(CONFIG_DIR, 'logging.json')
TENANTS_CONFIG_FILE = os.path.join(CONFIG_DIR, 'tenants.json')

def get(section, option=None, type=None):

    config = configparser.ConfigParser()

    with open(CONFIG_FILE, "r") as fp:
        config.read_file(fp)

        if option:

            try:
                value = config.get(section, option)
            except configparser.NoOptionError as exc:
                raise errors.ConfigurationError(exc)
            else:

                if type and hasattr(type, '__call__'):
                    return type(value)
                else:
                    return value
        else:

            try:
                data = dict(config.items(section))
            except configparser.NoSectionError as exc:
                raise errors.ConfigurationError(exc)
            else:
                return data
