def get_chunk(seq: list, n_chunks: int) -> list:
    """
    Divides given sequence to n chunks
    """
    seq_len = len(seq)
    if n_chunks > seq_len:
        raise ValueError(f"The number of chunks ({n_chunks}) exceeds the"
                         f" length of the sequence ({seq_len})")
    chunk_len = seq_len // n_chunks + bool(seq_len % n_chunks)
    return [seq[i * chunk_len:chunk_len * (i + 1)]
            for i in range(n_chunks)
            if seq[i * chunk_len:chunk_len * (i + 1)]]
