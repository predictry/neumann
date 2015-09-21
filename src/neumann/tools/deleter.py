import time
import os
import os.path
import json
import stomp
from neumann.utils import config
from neumann.utils.logger import Logger
from neumann.core import aws

class DeleteEventListener(stomp.ConnectionListener):

    def on_error(self, headers, message):
        Logger.error('Received an error message {0}'.format(message))

    def on_message(self, headers, message):
        Logger.info('Received a message {0}'.format(message))
        delete_event = json.loads(message)
        targetTenant = delete_event['tenantId']
        targetId = delete_event['id']
        changedFile = 0
        Logger.info('Receiving tenantId [{0}], id = [{1}]'.format(targetTenant, targetId))
        if targetTenant in config.get("output", "targettenants").split():
            # Alter json files
            outputDir = config.get("output", "dir")
            for root, dirs, files in os.walk(outputDir):
                for fname in [f for f in files if f.endswith('.json')]:
                    fpath = os.path.join(root, fname)
                    if fname == '{0}.json'.format(targetId):
                        os.remove(fpath)
                    else:
                        with open(fpath) as f:
                            s = f.read()
                        if targetId in s:
                            s = s.replace('"{0}"'.format(targetId), "").replace(", ]", "]").replace("[ ,", "[").replace(", ,", ",")
                            with open(fpath, "w") as f:
                                f.write(s)
                            changedFile += 1
            Logger.info('{0} files updated'.format(changedFile))

            # Sync to S3
            s3 = config.get("s3")
            s3bucket = s3["bucket"]
            s3path = os.path.join(s3["folder"], targetTenant, "recommendations")
            s3source = os.path.join(outputDir, s3["folder"], targetTenant, "recommendations")
            print("source [{0}] s3 path [{1}]".format(s3source, s3path))
            aws.S3.sync(s3source, s3bucket, s3path)
            Logger.info('Done syncing {0} to S3 {1}.'.format(s3source, s3path))
        else:
            Logger.error('Tenant [{0}] is not listed as target output'.format(targetTenant))


def main():
    host_and_port = (config.get("stomp", "host"), config.get("stomp", "port", int))
    Logger.info('Trying to connect to message queue in {0}.'.format(host_and_port))
    conn = stomp.Connection(host_and_ports=[host_and_port])
    conn.set_listener('', DeleteEventListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe('/topic/FISHER.DELETE_ITEM', 1)
    Logger.info('Deleter script has been subscribed to queue.')
    while True:
        time.sleep(1)

if __name__ == '__main__':
    logging = config.get("logging")
    path = os.path.join(config.PROJECT_BASE, logging["logconfig"])
    Logger.setup_logging(path)

    main()
