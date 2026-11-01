"""Chunking for P2 outbox sending.

Splits a reply into sends of at most MAX_CHARS characters, preferring
paragraph and line boundaries, never splitting a single line in the
middle of a word unless the line itself exceeds MAX_CHARS (then hard
split). No chunk numbering is added: chunks are sent in order and
read as one continuous reply in WeChat.

MAX_CHARS is a conservative local budget, not a verified iLink limit:
the official protocol doc read in P1 does not state a sendmessage
text limit, so 900 is chosen to stay well under typical message
limits while keeping long replies readable.
"""
MAX_CHARS = 900


def chunk_text(text: str, max_chars: int = MAX_CHARS) -> list[str]:
    if not text:
        return []
    chunks: list[str] = []
    current = ""
    for paragraph in text.split("\n"):
        # Preserve blank lines as paragraph separators when possible.
        piece = paragraph + "\n"
        if len(current) + len(piece) <= max_chars:
            current += piece
            continue
        if current:
            chunks.append(current.rstrip("\n"))
            current = ""
        # Paragraph itself too long: hard split, word-safe where possible.
        while len(piece) > max_chars:
            cut = piece.rfind(" ", 0, max_chars)
            if cut <= 0:
                cut = max_chars
            chunks.append(piece[:cut].rstrip())
            piece = piece[cut:].lstrip()
        current = piece
    if current:
        chunks.append(current.rstrip("\n"))
    return [c for c in chunks if c]


if __name__ == "__main__":
    import sys
    print(len(chunk_text(sys.stdin.read())))
