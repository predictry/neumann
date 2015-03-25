__author__ = 'guilherme'


from neumann.utils import config
from neumann.utils.logger import Logger

logging = config.get("logging")

Logger.setup_logging(logging["logconfig"])
