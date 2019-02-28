import os
from contextlib import contextmanager
from cloudstorageio.service.google_storage_interface import GoogleStorageInterface
from cloudstorageio.service.s3_interface import S3Interface
from cloudstorageio.service.local_storage_interface import LocalStorageInterface

class CloudInterface:

    def __init__(self):
        self._filename = None
        self._mode = None

    @staticmethod
    def is_local_path(path: str) -> bool:
        return os.path.exists(path)

    @staticmethod
    def is_s3_path(path: str) -> bool:
        if path.startswith(S3Interface.PREFIX):
            return True
        return False

    @staticmethod
    def is_google_storage_path(path: str) -> bool:
        if path.startswith(GoogleStorageInterface.PREFIX):
            return True
        return False

    def open(self, filename, mode):
        self._filename = filename
        self._mode = mode

        if self.is_local_path(path=filename):
            return LocalStorageInterface().open(self._filename, self._mode)

        elif self.is_google_storage_path(path=filename):
            return GoogleStorageInterface().open(self._filename, self._mode)

        elif self.is_s3_path(path=filename):
            return S3Interface().open(self._filename, self._mode)

        else:
            raise FileNotFoundError('No such file or directory: {}'.format(filename))


if __name__ == "__main__":
    google_file_path = "gs://test-cloudstorageio/sample-files/sample.txt"
    s3_file_path = 's3://test-cloudstorageio/sample.txt'
    local_file_path = '/home/vahagn/Documents/aws.csv'
    sample_local_file_path = '/home/vahagn/Documents/sample.txt'

    ci = CloudInterface()
    with open(sample_local_file_path, 'r') as f:
        res = f.read()
    with ci.open(google_file_path, 'w') as f:
        f.write(res)


