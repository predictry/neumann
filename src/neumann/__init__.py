import os.path

from neumann.utils import config
from neumann.utils.logger import Logger

logging = config.get("logging")
path = os.path.join(config.PROJECT_BASE, logging["logconfig"])
Logger.setup_logging(path)
