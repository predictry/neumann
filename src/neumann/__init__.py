__author__ = 'guilherme'


from neumann.utils import config
from neumann.utils.logger import Logger

conf = config.load_configuration()

if conf:
    Logger.setup_logging(conf["log_config_file"])
