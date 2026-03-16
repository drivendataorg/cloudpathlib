from pathlib import PurePosixPath
import re
from typing import Optional, Pattern


def _glob_has_magic(pattern: str) -> bool:
    """Check if a pattern has glob magic characters."""
    return bool(re.search(r"[*?\[\]]", pattern))


def _get_glob_prefix(pattern: str) -> str:
    """Extract the longest static prefix from a glob pattern.

    Returns the portion of the pattern before any glob magic characters.
    This prefix can be used to filter results server-side before applying
    the full glob pattern client-side.

    Examples:
        "foo/bar/*.txt" -> "foo/bar/"
        "*.txt" -> ""
        "foo/bar/baz.txt" -> "foo/bar/baz.txt"
        "foo/**/bar" -> "foo/"
    """
    parts = PurePosixPath(pattern).parts
    prefix_parts = []
    for part in parts:
        if _glob_has_magic(part):
            break
        prefix_parts.append(part)

    if not prefix_parts:
        return ""

    prefix = "/".join(prefix_parts)
    if len(prefix_parts) < len(parts):
        prefix += "/"

    return prefix


def _glob_to_regex(pattern: str, case_sensitive: Optional[bool] = None) -> Pattern[str]:
    """Convert a glob pattern to a compiled regex pattern.

    Supports:
        * - matches any characters except /
        ** - matches zero or more complete path segments
        ? - matches any single character except /
        [seq] - matches any character in seq
        [!seq] - matches any character not in seq

    Args:
        pattern: The glob pattern to convert
        case_sensitive: If True, match case-sensitively. If False, case-insensitive.
                       If None, defaults to True.

    Returns:
        A compiled regex pattern
    """
    if case_sensitive is None:
        case_sensitive = True

    regex_parts = []
    i = 0
    n = len(pattern)

    while i < n:
        c = pattern[i]

        if c == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                before_ok = i == 0 or pattern[i - 1] == "/"
                after_ok = i + 2 >= n or pattern[i + 2] == "/"

                if before_ok and after_ok:
                    if i + 2 < n and pattern[i + 2] == "/":
                        # **/rest — zero or more path segments before the rest
                        regex_parts.append("(.*/)?")
                        i += 3
                    else:
                        # ** at end of pattern (e.g. "a/**" or just "**")
                        if regex_parts and regex_parts[-1] == "/":
                            regex_parts.pop()
                            regex_parts.append("(/.*)?")
                        else:
                            regex_parts.append(".*")
                        i += 2
                else:
                    regex_parts.append("[^/]*[^/]*")
                    i += 2
            else:
                regex_parts.append("[^/]*")
                i += 1

        elif c == "?":
            regex_parts.append("[^/]")
            i += 1

        elif c == "[":
            j = i + 1
            if j < n and pattern[j] == "!":
                regex_parts.append("[^")
                j += 1
                # ] immediately after [! is a literal ]
                if j < n and pattern[j] == "]":
                    regex_parts.append("]")
                    j += 1
            elif j < n and pattern[j] == "]":
                regex_parts.append("[")
                regex_parts.append("]")
                j += 1
            else:
                regex_parts.append("[")

            # Glob and regex share [] syntax — pass through without escaping
            while j < n and pattern[j] != "]":
                if pattern[j] == "\\":
                    regex_parts.append(pattern[j : j + 2])
                    j += 2
                else:
                    regex_parts.append(pattern[j])
                    j += 1

            if j < n:
                regex_parts.append("]")
                i = j + 1
            else:
                regex_parts = regex_parts[:-1]
                regex_parts.append("\\[")
                i += 1

        elif c == "/":
            regex_parts.append("/")
            i += 1

        else:
            regex_parts.append(re.escape(c))
            i += 1

    regex_str = "^" + "".join(regex_parts) + "$"
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(regex_str, flags)
