class LocalStorageInterface:

    @staticmethod
    def open(file, mode: str = 'rt', *args, **kwargs):
        return open(file=file, mode=mode, *args, **kwargs)


if __name__ == "__main__":
    local_file_path = '/home/vahagn/Documents/aws.csv'

    ci = LocalStorageInterface()

    with ci.open(local_file_path) as f:
        content = f.read()
    print(content)
