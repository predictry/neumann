import time
import os
import os.path
import stomp
from neumann.message.command import CommandEventListener
from neumann.utils import config
from neumann.utils.logger import Logger


def main():
    host_and_port = (config.get("stomp", "host"), config.get("stomp", "port", int))
    Logger.info('Trying to connect to message queue in {0}.'.format(host_and_port))
    conn = stomp.Connection(host_and_ports=[host_and_port])
    conn.set_listener('', CommandEventListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe('/queue/NEUMANN_EC2.COMMAND', 1)
    Logger.info('Message script has been subscribed to queue.')
    while True:
        time.sleep(1)

if __name__ == '__main__':
    logging = config.get("logging")
    path = os.path.join(config.PROJECT_BASE, logging["logconfig"])
    Logger.setup_logging(path)

    main()
