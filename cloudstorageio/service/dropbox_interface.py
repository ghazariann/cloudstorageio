from typing import Union, Optional
from dropbox.files import FileMetadata, FolderMetadata, WriteMode
from dropbox.exceptions import ApiError

import dropbox

from cloudstorageio.utils.logger import logger


class DropBoxInterface:
    def __init__(self, token: str):
        self._encoding = 'utf8'
        self._mode = None
        self._current_path = None
        self._write_mode = None
        self.metadata = None
        self.path = None

        self._toke = token
        self.dbx = dropbox.Dropbox(token)

    @property
    def path(self):
        if self._current_path is None:
            raise ValueError("Path name is not set")
        return self._current_path

    @path.setter
    def path(self, value):
        if value is None:
            self._current_path = None
        else:
            self._current_path = '/' + value

    def _detect_path_type(self):
        """ Detect whether given path is file, folder or does not exists
        :return:
        """

        try:
            self.metadata = self.dbx.files_get_metadata(self.path)
        except ApiError:
            pass

        if isinstance(self.metadata, FileMetadata):
            self._isfile = True
        if isinstance(self.metadata, FolderMetadata):
            self._isdir = True

    def _populate_listdir(self):
        try:
            folder_metadata_list = self.dbx.files_list_folder(self.path).entries
            self._listdir = [f.name for f in folder_metadata_list]
        except ApiError:
            pass

    def _analyse_path(self, path: str):
        """
        :param path:
        :return:
        """

        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False

        self.path = path
        self._detect_path_type()
        self._populate_listdir()

        if self._isdir or self._isfile:
            self._object_exists = True

    def isfile(self, path: str):
        self._analyse_path(path)
        return self._isfile

    def isdir(self, path: str):
        self._analyse_path(path)
        return self._isdir

    def listdir(self, path: str):
        """Check given dictionary type and list content
        :param path: full path of s3 object (file/folder)
        :return:
        """
        self._analyse_path(path)
        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f"Not a directory: {path}")

        return self._listdir

    def remove(self, path: str):
        """Check given path type and remove object(s) if found any
        :param path: full path of s3 object (file/folder)
        :return:
        """
        self._analyse_path(path)
        if not self._object_exists:
            raise FileNotFoundError(f"Object with path {path} does not exists")

        self.dbx.files_delete(self.path)

        # for obj in self._object_summary_list:
        #     obj.delete()

    def open(self, path: str, mode: Optional[str] = None, *args, **kwargs):
        """Open a file from dropbox and return the DropBoxInterface object"""
        self._mode = mode
        self._analyse_path(path)

        return self

    def write(self, content: Union[str, bytes], metadata: Optional[dict] = None):

        """ Write content to file on dropBox
        :param content: The content that should be written to a file
        :param metadata:
        :return: String content of the file specified in the file path argument
        """
        if self._isfile:
            self._write_mode = WriteMode.overwrite
            logger.info('Overwriting {} file'.format(self.path))

        if isinstance(content, str):
            content = content.encode('utf8')
        if not metadata:
            metadata = {}
        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")

        self.dbx.files_upload(f=content, path=self.path, mode=self._write_mode)

    def read(self) -> Union[str, bytes]:
        """ Read dropBox file and return the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError('No such file: {}'.format(self.path))

        metadata, response = self.dbx.files_download(path="/sample.txt")
        res = response.content

        if self._mode is not None and 'b' not in self._mode:
            try:
                res = res.decode(self._encoding)
            except UnicodeDecodeError:
                raise ValueError(f"The content cannot be decoded into a string"
                                 f" with encoding {self._encoding}."
                                 f" Include 'b' on read mode to return the original bytes")
        return res

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None


if __name__ == '__main__':
    t = 'LiDj2MGlmdAAAAAAAAAAovO4Ck0PTSrIk6ZBFVZQxQ5ahdgs3_ILrYjGnw06pWLk'
    di = DropBoxInterface(token=t)
    with di.open('sample_folder/node1/node1.txt', 'wb') as f:
        f.write('lorem ipsum')

    di.remove('sample_folder/node1')