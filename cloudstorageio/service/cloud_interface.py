""" Class CloudInterface handles with Google Cloud Storage and S3 storage files (combination of GoogleStorageInterface\
    and S3Interface classes)

    Class CloudInterface has 'open' method, which is similar to python built-in 'open' method with more
    functionality(handles with s3 and google storage path also)

    See the followings for more information about features used in code:
        https://www.youtube.com/watch?v=-aKFBoZpiqA&t=596sP
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
        https://googleapis.github.io/google-cloud-python/latest/storage/
"""

import os
from cloudstorageio.service.google_storage_interface import GoogleStorageInterface
from cloudstorageio.service.s3_interface import S3Interface
from cloudstorageio.service.local_storage_interface import LocalStorageInterface


class CloudInterface:

    def __init__(self):
        self._filename = None
        self._mode = None
        self._s3 = None
        self._gs = None
        self._local = None
        self._current_storage = None

    def identify_path_type(self, path: str):
        """Identify type of the path and make assignments according to that type
        :param path: full path of file
        :return: None
        """
        if self.is_local_path(path):
            if self._local is None:
                self._local = LocalStorageInterface()
            self._current_storage = self._local
        elif self.is_s3_path(path):
            if self._s3 is None:
                self._s3 = S3Interface()
            self._current_storage = self._s3
        elif self.is_google_storage_path(path):
            if self._gs is None:
                self._gs = GoogleStorageInterface()
            self._current_storage = self._gs
        else:
            raise ValueError(f"`{path}` is invalid")

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

    def open(self, file, mode='rt', *args, **kwargs):
        self.identify_path_type(file)
        return self._current_storage.open(file=file, mode=mode, *args, **kwargs)


if __name__ == "__main__":
    google_file_path = "gs://test-cloudstorageio/sample-files/sample_1.txt"
    s3_file_path = 's3://test-cloudstorageio/sample.txt'
    local_file_path = '/home/vahagn/Documents/aws.csv'
    sample_local_file_path = '/home/vahagn/Documents/sample.txt'

    ci = CloudInterface()

    with ci.open(s3_file_path, 'r') as f:
        a = f.read()
    print(a)
    # with ci.open(google_file_path, 'w') as f:
    #     f.write(res)
