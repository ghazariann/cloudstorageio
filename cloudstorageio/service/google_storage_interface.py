from google.cloud import storage


class GoogleStorageInterface:
    prefix = "gs://"

    def __init__(self, configs=None):
        self.configs = configs


    storage_client = storage.Client()

    buckets = list(storage_client.list_buckets())
    print(buckets)
