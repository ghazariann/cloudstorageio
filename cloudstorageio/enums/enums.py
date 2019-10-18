import enum


class PrefixEnums(enum.Enum):
    S3 = 's3://'
    GOOGLE_CLOUD = 'gs://'
    DROPBOX = 'dbx://'
    GOOGLE_DRIVE = 'gdrive://'
