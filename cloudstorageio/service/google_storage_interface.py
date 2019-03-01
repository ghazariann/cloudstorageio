import io
from contextlib import contextmanager
from typing import Tuple, Union, Optional

from google.cloud import storage


class GoogleStorageInterface:
    PREFIX = "gs://"

    def __init__(self):
        self._encoding = 'utf8'
        self._storage_client = storage.Client()
        self._mode = None
        self._current_bucket = None
        self._current_path = None
        self._bucket = None
        self._blob = None
        self.path = None

    @property
    def path(self):
        if self._current_bucket is None:
            raise ValueError("Bucket name is not set")
        if self._current_path is None:
            raise ValueError("Path name is not set")
        return f"{GoogleStorageInterface.PREFIX}{self._current_bucket}/{self._current_path}"

    @path.setter
    def path(self, value):
        if value is None:
            self._current_path = None
            self._current_bucket = None
        else:
            self._current_bucket, self._current_path = self._parse_bucket(value)

    @contextmanager
    def open(self, file: str, mode: Optional[str] = None, *args, **kwargs):
        """Open a file from gs and return the GoogleStorageInterface object"""
        self._mode = mode
        self.path = file
        try:
            yield self
        finally:
            self.path = None

    def read(self) -> Union[str, bytes]:
        """ Read gs file and return the bytes
        :return: String content of the file
        """
        self._bucket = self._storage_client.get_bucket(self._current_bucket)
        self._blob = self._bucket.get_blob(self._current_path)
        res = self._blob.download_as_string()

        if self._mode is not None and 'b' not in self._mode:
            try:
                res = res.decode(self._encoding)
            except UnicodeDecodeError:
                raise ValueError(f"The content cannot be decoded into a string"
                                 f" with encoding {self._encoding}."
                                 f" Include 'b' on read mode to return the original bytes")
        return res

    def write(self, content: Union[str, bytes, io.IOBase]):
        """ Write text to a file on google storage
        :param content: The content that should be written to a file
        :param metadata:
        :return: String content of the file specified in the filepath argument
        """
        if isinstance(content, str):
            content = content.encode('utf8')

        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")
        self._bucket = self._storage_client.get_bucket(self._current_bucket)
        blob = self._bucket.blob(self._current_path)
        blob.upload_from_string(content)

    @staticmethod
    def _parse_bucket(path: str) -> Tuple[str, str]:
        """Given a path, return the bucket name and the filepath as a tuple"""
        path = path.split(GoogleStorageInterface.PREFIX, 1)[-1]
        bucket_name, path = path.split('/', 1)
        return bucket_name, path


if __name__ == '__main__':

    local_file_path = '/home/vahagn/Documents/sample.txt'
    google_path_example = "gs://test-cloudstorageio/sample-files/sample.txt"

    gs = GoogleStorageInterface()

    with open(local_file_path, 'rb') as local_f:
        cont = local_f.read()
        with gs.open(google_path_example, 'w') as f:
            f.write(cont)
    # print(output)
