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


def get_chunk(seq: list, n_chunks: int, sort: bool = True) -> list:
    """
    Divides given sequence to n chunks
    """
    if sort is True:
        seq = sorted(seq)

    seq_len = len(seq)
    if n_chunks > seq_len:
        raise ValueError(f"The number of chunks ({n_chunks}) exceeds the"
                         f" length of the sequence ({seq_len})")
    chunk_len = seq_len // n_chunks + bool(seq_len % n_chunks)
    return [seq[i * chunk_len:chunk_len * (i + 1)]
            for i in range(n_chunks)
            if seq[i * chunk_len:chunk_len * (i + 1)]]