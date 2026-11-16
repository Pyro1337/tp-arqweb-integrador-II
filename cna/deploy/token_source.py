"""Read a rotating Gitea credential per operation, without restarting a service.

Mount the containing directory read-only, not the individual file (atomic rename).
No token value is placed in exceptions or logs. Explicit file errors fail closed.
"""
import os
import re
import stat


def gitea_headers(fallback):
    path = os.getenv("GITEA_TOKEN_FILE", "")
    token = fallback
    if path:
        fd = None
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= 8192:
                raise ValueError("invalid token source")
            token = os.read(fd, 8193).decode("ascii").strip()
            if not re.fullmatch(r"[A-Za-z0-9._~-]{1,8192}", token):
                raise ValueError("invalid token source")
        except (OSError, UnicodeError, ValueError):
            raise ValueError("Gitea token file unavailable or invalid") from None
        finally:
            if fd is not None:
                os.close(fd)
    return {"Authorization": "token " + token} if token else {}
