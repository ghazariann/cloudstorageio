import os
from contextlib import contextmanager
from cloudstorageio.service.google_storage_interface import GoogleStorageInterface
from cloudstorageio.service.s3_interface import S3Interface


class CloudInterface:
    def __init__(self, path, aws_configs=None, google_configs=None):
        #if aws_configs:
        self.s3_interface = S3Interface(configs=aws_configs, path=path)
        if google_configs:
            self.gs_interface = GoogleStorageInterface(configs=google_configs)

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
        if path.startswith(GoogleStorageInterface.prefix):
            return True
        return False

    @contextmanager
    def open(self, path: str):
        if self.is_local_path(path):
            try:
                file = open(path)
                yield file

            finally:
                file.close()

        elif self.is_google_storage_path(path):
            pass

        elif self.is_s3_path(path):
            file = self.s3_interface
            yield file


# Test
if __name__ == "__main__":
    google_path_example = "gs://file/path"
    s3_path_example = "s3://account-classification-david/PDF/jj keller.pdf"
    local_file_path = '/home/vahagn/Documents/aws.csv'

    ci = CloudInterface(s3_path_example)

    with ci.open(s3_path_example) as f:
        content = f.read()
    print(content)

