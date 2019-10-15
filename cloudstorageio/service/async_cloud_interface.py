import functools
import itertools
import multiprocessing
import os
import queue
from multiprocessing.pool import Pool

from typing import Optional
from threading import Thread
from cloudstorageio import CloudInterface
from cloudstorageio.utils.decorators import timer
from cloudstorageio.utils.logger import logger


class AsyncCloudInterface:

    def __init__(self, aws_region_name: Optional[str] = None, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, dropbox_token: Optional[str] = None,
                 dropbox_root: Optional[bool] = False, google_cloud_credentials_path: Optional[str] = None,
                 google_drive_credentials_path: Optional[str] = None, **kwargs):

        """Initializes AsyncCloudInterface instance
        :param aws_region_name: region name for S3 storage
        :param aws_access_key_id: access key id for S3 storage
        :param aws_secret_access_key: secret access key for S3 storage
        :param dropbox_token: generated token for dropbox app access
        :param google_cloud_credentials_path: local path of google cloud credentials file (json)
        :param google_drive_credentials_path: local path of google drive secret credentials file (json)
        :param kwargs:
        """

        self._kwargs = kwargs
        self.aws_region_name = aws_region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.dropbox_token = dropbox_token
        self.dropbox_root = dropbox_root
        self.google_cloud_credentials_path = google_cloud_credentials_path
        self.google_drive_credentials_path = google_drive_credentials_path

    def read_files(self, file_path_list: list, q: queue.Queue, from_folder_path: str):
        """Reads given files and put content in given queue
        :param file_path_list: list of files' path
        :param q: queue which contains file path and file content
        :param from_folder_path: folder path of files to read
        :return:
        """
        ci = CloudInterface(aws_region_name=self.aws_region_name, aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key, dropbox_token=self.dropbox_token,
                            dropbox_root=self.dropbox_root,
                            google_cloud_credentials_path=self.google_cloud_credentials_path,
                            google_drive_credentials_path=self.google_drive_credentials_path)

        for file_path in file_path_list:
            q.put((file_path, ci.fetch(os.path.join(from_folder_path, file_path))))

        q.put(('', ''))

    def write_files(self, q: queue.Queue, from_folder_path: str, to_folder_path: str):
        """Writes given file contents to new files
        :param q: queue which contains file path and file content
        :param from_folder_path:folder path of already read files
        :param to_folder_path: folder path of files to write
        :return:
        """
        while True:
            ci = CloudInterface(aws_region_name=self.aws_region_name, aws_access_key_id=self.aws_access_key_id,
                                aws_secret_access_key=self.aws_secret_access_key, dropbox_token=self.dropbox_token,
                                dropbox_root=self.dropbox_root,
                                google_cloud_credentials_path=self.google_cloud_credentials_path,
                                google_drive_credentials_path=self.google_drive_credentials_path)

            file_path, content = q.get(block=True)
            # Use Enum for identifying queue progress
            if not file_path:
                break

            to_file_path = os.path.join(to_folder_path, file_path)
            ci.save(to_file_path, content)
            logger.info(f"Copied file {os.path.join(from_folder_path, file_path)} to {to_file_path}")
            q.task_done()

    @staticmethod
    def get_chunk(seq: list, n_chunks: int) -> list:
        """
        Divides given sequence to n chunks
        """
        seq_size = len(seq)
        result_list = list()
        if n_chunks > seq_size:
            raise ValueError(f"The number of chunks ({n_chunks}) exceeds the"
                             f" length of the sequence ({seq_size})")

        [result_list.append([]) for _ in range(n_chunks)]
        for idx, chunk in enumerate(itertools.cycle(result_list)):
            if idx == seq_size:
                break
            chunk.append(seq[idx])

        return result_list

    def copy_batch(self, from_path: str, to_path: str, continue_copy: Optional[bool] = False,
                   process_amount: int = 10):
        """ Asynchronous copy entire batch(folder) to new destination
        :param from_path: folder/bucket to copy from
        :param to_path: name of folder to copy files
        :param continue_copy:
        :param process_amount:
        :return:
        """

        ci = CloudInterface(aws_region_name=self.aws_region_name, aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key, dropbox_token=self.dropbox_token,
                            dropbox_root=self.dropbox_root,
                            google_cloud_credentials_path=self.google_cloud_credentials_path,
                            google_drive_credentials_path=self.google_drive_credentials_path)
        if continue_copy:
            from_path_list = ci.listdir(from_path, recursive=True)
            try:
                to_path_list = ci.listdir(to_path, recursive=True)
            except FileNotFoundError:
                to_path_list = []

            full_path_list = list(set(from_path_list) - set(to_path_list))
        else:
            full_path_list = ci.listdir(from_path, recursive=True)

        if process_amount > len(full_path_list):
            process_amount = len(full_path_list)

        chunks = self.get_chunk(full_path_list, process_amount)

        read_threads = []
        write_threads = []

        for chunk in chunks:
            q = queue.Queue()

            read_thread = Thread(target=self.read_files,
                                 args=(chunk, q, from_path))

            read_threads.append(read_thread)

            write_thread = Thread(target=self.write_files, args=(q, from_path, to_path))
            write_threads.append(write_thread)

            read_thread.start()
            write_thread.start()

        # join all opened threads
        for r, w in zip(read_threads, write_threads):
            r.join()
            w.join()
