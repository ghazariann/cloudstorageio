from contextlib import contextmanager
file_path = '/home/vahagn/Documents/aws.csv'


class File:

    def __init__(self, filename):
        self.filename = filename

    @staticmethod
    @contextmanager
    def open(filename):
        try:
            f = open(filename)
            yield f

        finally:
            f.close()


ci = File(file_path)
with ci.open(file_path) as file:
    o = file.read()
print(o)