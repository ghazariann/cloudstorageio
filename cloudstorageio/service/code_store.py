@staticmethod
def _subset_split(text: str) -> list:
    """
    :param text:
    :return:
    """
    a = list()
    for i in range(len(text.split('/'))):
        a.append(text.rsplit('/', i)[0])
    return a
  # self._object_exists = storage.Blob(bucket=self._bucket, name=self._current_path).exists(self._storage_client)

        # if self._blob._properties['contentType'] == 'text/plain':
        #     self._isfile = True
        # if self._blob._properties['contentType'] == 'folder':
        #     self._isfile = True

# if object_summary.key == self._current_path:
#     if object_summary.get()['ContentType'] == 'binary/octet-stream':
#         self._isfile = True
# else:
#     # if object_summary.get()['ContentType'] == 'application/x-directory':
#     self._isdir = True
