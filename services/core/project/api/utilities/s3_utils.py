import os
import boto
import boto3
import mimetypes
import urllib
from boto.s3.key import Key
from botocore.client import Config
from project.api.utilities.constants import aws_access_key_id, aws_secret_access_key, aws_region, aws_client_data_bucket, \
    aws_region_prefix

env = os.environ.get('FLASK_ENV')
env = 'prod' if env == 'production' else 'qa'


def process_upload_logo_file(client_prefix, file_ref, bucket="client_logos", file_name=None, master_bucket=None):
    file_ext = (file_ref.filename).split('.')[-1]
    if file_name:
        file_key = '/%s/%s.%s' % (bucket, client_prefix+'_'+env + file_name, file_ext)
    else:
        file_key = '/%s/%s.%s' % (bucket, client_prefix+'_'+env + '_logo_file', file_ext)
    status = upload_to_s3(file_ref, aws_client_data_bucket, file_key, aws_region)
    if not status:
        raise Exception('Failed to upload the file...')
    file_url = 'https://%s/%s%s' % (aws_region, master_bucket if master_bucket else aws_client_data_bucket, urllib.parse.quote(file_key))
    return file_url


def upload_to_s3(file, bucket_name, key, aws_region_host):
    try:
        size = os.fstat(file.fileno()).st_size
    except Exception as e:
        print(e)
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key, host=aws_region_host)
    bucket = conn.get_bucket(bucket_name, validate=False)
    k = Key(bucket)
    k.key = key
    k.set_metadata('Content-Type', mimetypes.guess_type(key)[0] or "application/octet-stream")
    sent = k.set_contents_from_file(file, cb=None, md5=None, reduced_redundancy=False, rewind=True)

    # Rewind for later use
    file.seek(0)
    if sent == size:
        uploaded_key = bucket.lookup(key)
        uploaded_key.set_acl('public-read')
        return True
    return False


