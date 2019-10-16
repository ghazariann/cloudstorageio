def add_slash(text: str):
    """returns the same text with slash at the end"""
    return text + '/'


def path_formatter(path: str) -> str:
    """returns the same path with valid structure"""
    striped_parts = [word.strip() for word in path.split('/')]
    return '/'.join(striped_parts)


def str2bool(bool_string: str):
    """Detects bool type from string"""
    if not bool_string:
        return None
    return bool_string.lower() in ("yes", "true", "1")
