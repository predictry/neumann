import os
import os.path
import configparser

from neumann.core import errors

PROJECT_BASE = './'
CONFIG_DIR = None
for config_dir in os.path.expanduser('~/.local/share/neumann/config'), os.path.join(os.curdir, 'config', 'dev'), \
                  os.path.join(os.curdir, 'config', 'prod'), '/etc/neumann':
    CONFIG_DIR = config_dir
    if os.path.isdir(CONFIG_DIR) and os.path.exists(CONFIG_DIR):
        break

CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.ini')
TASK_CONFIG_FILE = os.path.join(CONFIG_DIR, 'tasks.ini')
LOGGING_CONFIG_FILE = os.path.join(CONFIG_DIR, 'logging.json')
TENANTS_CONFIG_FILE = os.path.join(CONFIG_DIR, 'tenants.json')

config = configparser.ConfigParser()
with open(CONFIG_FILE, 'r') as fp:
    config.read_file(fp)


def get(section, option=None, the_type=None):

    if option:

        try:
            value = config.get(section, option)
        except configparser.NoOptionError as exc:
            raise errors.ConfigurationError(exc)
        else:

            if the_type and hasattr(the_type, '__call__'):
                return the_type(value)
            else:
                return value
    else:

        try:
            data = dict(config.items(section))
        except configparser.NoSectionError as exc:
            raise errors.ConfigurationError(exc)
        else:
            return data
