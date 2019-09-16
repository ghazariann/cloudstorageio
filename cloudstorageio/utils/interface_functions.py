def add_slash(text: str):
    """returns the same text with slash at the end"""
    return text + '/'


def path_formatter(path: str) -> str:
    """returns the same path with valid structure"""
    striped_parts = [word.strip() for word in path.split('/')]
    return '/'.join(striped_parts)