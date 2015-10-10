import os
import os.path
import stomp
import ujson
from neumann.utils import config
from neumann.utils.logger import Logger
from neumann.core import aws


class DeleteEventListener(stomp.ConnectionListener):

    def on_error(self, headers, message):
        Logger.error('Received error for message [{0}] headers [{1}]'.format(message, headers))

    def on_message(self, headers, message):
        try:
            Logger.info('Received a message {0} headers {1}'.format(message, headers))
            delete_event = ujson.loads(message)
            target_tenant = delete_event['tenantId']
            target_id = delete_event['id']
            changed_file = 0
            Logger.info('Receiving tenantId [{0}], id = [{1}]'.format(target_tenant, target_id))
            if target_tenant in config.get("output", "targettenants").split():
                # Alter json files
                outputdir = os.path.join('/var/neumann', config.get("output", "dir"))
                for root, dirs, files in os.walk(outputdir):
                    for fname in [f for f in files if f.endswith('.json')]:
                        fpath = os.path.join(root, fname)
                        if fname == '{0}.json'.format(target_id):
                            os.remove(fpath)
                        else:
                            with open(fpath) as f:
                                data = ujson.load(f)
                            if ('items' in data) and (target_id in data['items']):
                                data["items"].remove(target_id)
                                with open(fpath, 'w') as f:
                                    ujson.dump(data, f)
                                changed_file += 1
                Logger.info('{0} files updated'.format(changed_file))

                # Sync to S3
                s3 = config.get("s3")
                s3bucket = s3["bucket"]
                s3path = os.path.join(s3["folder"], target_tenant, "recommendations")
                s3source = os.path.join(outputdir, s3["folder"], target_tenant, "recommendations")
                print("source [{0}] s3 path [{1}]".format(s3source, s3path))
                aws.S3.sync(s3source, s3bucket, s3path)
                Logger.info('Done syncing {0} to S3 {1}.'.format(s3source, s3path))
            else:
                Logger.error('Tenant [{0}] is not listed as target output'.format(target_tenant))
        except Exception as e:
            Logger.error("Unexception error: {0}".format(e))
            Logger.exception(e)
