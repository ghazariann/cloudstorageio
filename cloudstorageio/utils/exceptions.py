class CaseInsensitivityError(Exception):
    """cloudstorageio's CloudInterface is case sensitive, and uses CaseInsensitivityError exception to prevent
     conflicts and overwriting """
    pass
