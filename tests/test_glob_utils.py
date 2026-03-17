import re

import pytest

from cloudpathlib.glob_utils import _get_glob_prefix, _glob_has_magic, _glob_to_regex


@pytest.mark.parametrize(
    "pattern,expected",
    [
        ("plain/path", False),
        ("*.txt", True),
        ("file?.txt", True),
        ("name[0-9].txt", True),
    ],
)
def test_glob_has_magic(pattern, expected):
    assert _glob_has_magic(pattern) is expected


@pytest.mark.parametrize(
    "pattern,expected",
    [
        ("foo/bar/*.txt", "foo/bar/"),
        ("*.txt", ""),
        ("foo/bar/baz.txt", "foo/bar/baz.txt"),
        ("foo/**/bar", "foo/"),
        ("foo/[ab]ar.txt", "foo/"),
    ],
)
def test_get_glob_prefix(pattern, expected):
    assert _get_glob_prefix(pattern) == expected


@pytest.mark.parametrize(
    "pattern,matches,does_not_match",
    [
        ("*.txt", ["a.txt", "x.y.txt"], ["a/b.txt", "a.txt/b"]),
        ("**/file.txt", ["file.txt", "a/file.txt", "a/b/file.txt"], ["a/file.txt.bak"]),
        ("**", ["", "a", "a/b"], []),
        ("a/**", ["a", "a/b", "a/b/c"], ["ab", "x/a"]),
        ("file?.txt", ["file1.txt", "fileA.txt"], ["file12.txt", "file.txt"]),
        ("file[0-9].txt", ["file1.txt"], ["filex.txt"]),
        ("file[!0-9].txt", ["filex.txt"], ["file1.txt"]),
        ("ab**cd", ["abcd", "abZZcd"], ["ab/zz/cd"]),
        ("file[!]].txt", ["filea.txt"], ["file].txt"]),
        ("file[]a].txt", ["file].txt", "filea.txt"], ["fileb.txt"]),
        (r"file[a\]].txt", ["filea.txt", "file].txt"], ["fileb.txt"]),
    ],
)
def test_glob_to_regex_matching(pattern, matches, does_not_match):
    regex = _glob_to_regex(pattern)
    for candidate in matches:
        assert regex.match(candidate), f"{pattern} should match {candidate}"
    for candidate in does_not_match:
        assert not regex.match(candidate), f"{pattern} should not match {candidate}"


def test_glob_to_regex_case_insensitive():
    regex = _glob_to_regex("FILE*.TXT", case_sensitive=False)
    assert regex.flags & re.IGNORECASE
    assert regex.match("file_a.txt")
    assert regex.match("FiLe_A.TxT")


def test_glob_to_regex_unclosed_character_class_raises():
    with pytest.raises(re.PatternError):
        _glob_to_regex("foo[bar")
