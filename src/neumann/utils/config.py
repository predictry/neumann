__author__ = 'guilherme'

import os
import os.path
import ConfigParser

from neumann.core import errors

PROJECT_BASE = ''.join([os.path.dirname(os.path.abspath(__file__)), "/../../../"])
CONFIG_FILE = ''.join([PROJECT_BASE, 'config.ini'])


def get(section, option=None, type=None):

    config = ConfigParser.ConfigParser()

    with open(CONFIG_FILE, "r") as fp:
        config.readfp(fp)

        if option:

            try:
                value = config.get(section, option)
            except ConfigParser.NoOptionError as exc:
                raise errors.ConfigurationError(exc)
            else:

                if type and hasattr(type, '__call__'):
                    return type(value)
                else:
                    return value
        else:

            try:
                data = dict(config.items(section))
            except ConfigParser.NoSectionError as exc:
                raise errors.ConfigurationError(exc)
            else:
                return data
