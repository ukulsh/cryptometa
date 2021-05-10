import os
import boto
import boto3
import mimetypes
import urllib
from boto.s3.key import Key
from botocore.client import Config
from project.api.constants import aws_access_key_id, aws_secret_access_key, aws_region, aws_client_data_bucket, \
    aws_region_prefix

env = os.environ.get('FLASK_ENV')
env = 'prod' if env == 'production' else 'qa'


def get_presigned_url(url):
    try:
        if not url:
            return url
        url_prefix = 'https://%s/%s' % (aws_region, aws_client_data_bucket)
        key = url.split(url_prefix)[1][1:]
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                 region_name=aws_region_prefix, config=Config(signature_version='s3v4'))
        file_path = urllib.parse.unquote(key)
        url = s3_client.generate_presigned_url('get_object', Params={'Bucket': aws_client_data_bucket, 'Key': file_path},
                                               ExpiresIn=3600)
        return url
    except Exception:
        return url


def process_upload_file(client_prefix, file_ref, file_prefix):
    file_ext = (file_ref.filename).split('.')[-1]
    file_key = '/%s/%s/%s.%s' % (env, client_prefix, file_prefix, file_ext)
    status = upload_to_s3(file_ref, aws_client_data_bucket, file_key, aws_region)
    if not status:
        raise Exception('Failed to upload the file...')
    file_url = 'https://%s/%s%s' % (aws_region, aws_client_data_bucket, urllib.parse.quote(file_key))
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
        return True
    return False


