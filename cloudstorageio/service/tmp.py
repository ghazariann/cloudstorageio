import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata
t = 'LiDj2MGlmdAAAAAAAAAAovO4Ck0PTSrIk6ZBFVZQxQ5ahdgs3_ILrYjGnw06pWLk'
dpx = dropbox.Dropbox(t)


def is_file(file: str):
    try:
        metadata = dpx.files_get_metadata(file)
        if metadata:
            return True
        else:
            return False
    except ApiError:
        return False


if __name__ == '__main__':
    f = '/sample_folder/node1/node1.txt'
    print(is_file(f))