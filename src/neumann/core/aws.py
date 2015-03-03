__author__ = 'guilherme'

from boto.s3.connection import S3Connection
from boto.s3.key import Key


def upload_file_to_s3(bucket_name, key, fp):

    conn = S3Connection()

    bucket = conn.get_bucket(bucket_name)

    s3_key = Key(bucket)
    s3_key.key = key

    s3_key.set_contents_from_file(fp, replace=True)

    return