from google.cloud import storage
from google.cloud.storage import Blob

storage_client = storage.Client()

bucket = storage_client.get_bucket('test-cloudstorageio')
#
blob = bucket.get_blob('sample-files/sample.txt')
#
blob.upload_from_string("my secret message.")



