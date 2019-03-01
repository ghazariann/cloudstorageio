from google.cloud import storage
from google.cloud.storage import Blob

storage_client = storage.Client()

bucket = storage_client.get_bucket('test-cloudstorageio')
#
blob = bucket.blob('sample-files/sample4.txt')

# if blob is None:
#     blob = bucket.blob('sample-files/sample1.txt')

print(blob)
blob.upload_from_string("mysdzbssdfbyyy.")



