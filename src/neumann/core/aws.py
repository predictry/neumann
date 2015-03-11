__author__ = 'guilherme'

import os
import os.path
import subprocess
import sys

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from neumann.utils.logger import Logger


def upload_file_to_s3(bucket_name, key, fp):

    conn = S3Connection()

    bucket = conn.get_bucket(bucket_name)

    s3_key = Key(bucket)
    s3_key.key = key

    s3_key.set_contents_from_file(fp, replace=True)

    return


def sync(dir, bucket, s3path):

    s3path = os.path.join("s3://", bucket, s3path)

    awscli = os.path.join(os.path.abspath(os.path.join(sys.executable, os.pardir)), "aws")
    cmd = [awscli, "s3", "sync", dir, s3path]

    Logger.info("Running command:\t{0}".format(''.join(cmd)))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=False)

    output, err = p.communicate()

    if p.returncode == 1:

        msg = "Error running command:\n\t{0}".format(' '.join(cmd))
        Logger.error(msg)
        if output:
            Logger.error(output)
        if err:
            Logger.error(err)

        raise RuntimeError

    elif p.returncode == 0:

        Logger.info("Successfully executed command:\n\t{0}".format(' '.join(cmd)))

    return

