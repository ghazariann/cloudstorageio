""" Class DropBoxInterface handles with DropBoxInterface workspace files/folders

    Class DropBoxInterface has
                                    read and write methods (can be accessed by open method)
                                    isfile and isdir methods for checking object status (file, folder)
                                    listdir method for listing folder's content
                                    remove method for removing file/folder
"""

import os
import re

import dropbox
from typing import Union, Optional

from dropbox.common import PathRoot
from dropbox.files import FileMetadata, FolderMetadata, WriteMode
from dropbox.exceptions import ApiError
from dropbox.stone_validators import ValidationError

from cloudstorageio.configs import CloudInterfaceConfig
from cloudstorageio.enums.enums import PrefixEnums
from cloudstorageio.exceptions import CaseInsensitivityError
from cloudstorageio.tools.logger import logger
from cloudstorageio.tools.ci_collections import add_slash, str2bool


class DropBoxInterface:
    PREFIX = PrefixEnums.DROPBOX.value

    def __init__(self, **kwargs):
        """Initializes DropBoxInterface instance, creates dbx instance
        :param kwargs:
        """
        # try to find token from given kwargs arguments or from os environment
        self.token = kwargs.pop('dropbox_token', None)
        self.root = kwargs.pop('dropbox_root', None)

        if not self.token:
            self.token = os.environ.get('DROPBOX_TOKEN')
        if self.root is None:
            self.root = str2bool(os.environ.get('DROPBOX_ROOT'))
        if not self.root:
            self.root = False

        if not self.token:
            raise ValueError('Please specify dropbox app access key')

        self._encoding = 'utf8'
        self._mode = None
        self._current_path = None
        self._write_mode = None
        self.metadata = None
        self.path = None
        self.list_recursive = False
        self.include_folders = False

        self.dbx = dropbox.Dropbox(self.token)

        # namespace id starts from root
        if self.root:
            root_namespace_id = self.dbx.users_get_current_account().root_info.root_namespace_id
            self.dbx = self.dbx.with_path_root(PathRoot.namespace_id(root_namespace_id))

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
            value = value.split(self.PREFIX, 1)[-1]

            if value in ('.', ''):
                self._current_path = ''
            else:
                self._current_path = value if value.startswith('/') else f'/{value}'
                self._current_path = self._current_path[:-1] if self._current_path.endswith('/') else self._current_path

    def _detect_path_type(self):
        """Detects whether given path is file, folder or does not exists"""
        if self.path == '':
            self._isdir = True
        try:
            self.metadata = self.dbx.files_get_metadata(self.path)
        except (ApiError, ValidationError):
            self.metadata = None

        if isinstance(self.metadata, FileMetadata):
            self._isfile = True
        if isinstance(self.metadata, FolderMetadata):
            self._isdir = True

        if self._isdir or self._isfile:
            self._object_exists = True
            if self.metadata:
                if self.metadata.path_display != self.path and self.metadata.path_lower == self.path.lower():
                    raise CaseInsensitivityError(f'DropBox case-insensitivity conflict: The given  {self.path} is'
                                                 f' the same file(folder) as {self.metadata.path_display}')

    def _populate_listdir(self):
        """Appends each file.folder name to self._listdir"""

        def __populate_metadata(metadata):
            for f in metadata.entries:
                try:
                    full_path = re.split(add_slash(self.path), f.path_display, flags=re.IGNORECASE, maxsplit=1)[1]
                    if isinstance(f, FolderMetadata):
                        if self.include_folders:
                            self._listdir.append((add_slash(full_path)))
                    else:
                        self._listdir.append(full_path)
                except IndexError:
                    # failed to split path
                    continue

        folder_metadata = self.dbx.files_list_folder(self.path, recursive=self.list_recursive)

        __populate_metadata(metadata=folder_metadata)

        while folder_metadata.has_more:
            cur = folder_metadata.cursor
            folder_metadata = self.dbx.files_list_folder_continue(cur)
            __populate_metadata(metadata=folder_metadata)

        try:
            self._listdir.remove('')
        except ValueError:
            pass

    def _analyse_path(self, path: str):
        """From given path lists and detects object type (file/folder)"""

        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False
        self._write_mode = None

        self.path = path

        self._detect_path_type()

    def isfile(self, path: str):
        """Checks file existence for given path"""
        self._analyse_path(path)
        return self._isfile

    def isdir(self, path: str):
        """Checks dictionary existence for given path"""
        self._analyse_path(path)
        return self._isdir

    def listdir(self, path: str, recursive: Optional[bool] = False, exclude_folders: Optional[bool] = False):
        """Lists content for given folder path"""
        self.list_recursive = recursive
        self.include_folders = not exclude_folders
        self._analyse_path(path)
        if self._isdir:
            self._populate_listdir()

        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f"Not a directory: {path}")
        return self._listdir

    def remove(self, path: str):
        """Deletes file/folder"""
        self._analyse_path(path)
        if not self._object_exists:
            raise FileNotFoundError(f"Object with path {path} does not exists")

        self.dbx.files_delete_v2(self.path)

    def open(self, path: str, mode: Optional[str] = None):
        """Opens a file from dropBox and returns the DropBoxInterface object"""
        self._mode = mode
        self._analyse_path(path)

        return self

    def write(self, content: Union[str, bytes], metadata: Optional[dict] = None):
        """Writes content to file on dropBox
        :param content: The content that should be written to a file
        :param metadata:
        :return: String content of the file specified in the file path argument
        """

        if self._isfile:
            self._write_mode = WriteMode.overwrite
            # logger.info('Overwriting {} file'.format(self.path))

        if isinstance(content, str):
            content = content.encode('utf8')
        if not metadata:
            metadata = {}
        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")

        try:
            res = self.dbx.files_upload(f=content, path=self.path, mode=self._write_mode)
            if res.path_display != self.path and res.path_lower == self.path.lower():
                self.dbx.files_delete_v2(self.path)
                raise CaseInsensitivityError(f'DropBox case-insensitivity conflict: The given  {self.path} is'
                                             f' the same file(folder) as {res.path_display}')
        except ApiError:
            logger.info(f'Failed to upload {self.path} to dropbox')

    def read(self) -> Union[str, bytes]:
        """Reads dropBox file and returns the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError('No such file: {}'.format(self.path))

        metadata, response = self.dbx.files_download(path=self.path)
        res = response.content
        response.close()

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
    my_configs = '/home/vahagn/Dropbox/cognaize/cloudstorageio_creds.json'
    CloudInterfaceConfig.set_configs(config_json_path=my_configs)
    dr = DropBoxInterface()
    res = dr.listdir('dbx://')
    print(res)