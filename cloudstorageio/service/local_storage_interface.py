""" Class LocalStorageInterface handles with local files

    Class LocalStorageInterface  has open method, which is similar to python biult-in 'open' method
"""


class LocalStorageInterface:

    @staticmethod
    def open(file, mode: str = 'rt', *args, **kwargs):
        return open(file=file, mode=mode, *args, **kwargs)


if __name__ == "__main__":
    local_file_path = '/home/vahagn/Documents/aws.csv'

    ci = LocalStorageInterface()

    with ci.open(local_file_path, 'r') as f:
        content = f.read()
    print(content)
