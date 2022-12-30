#!/usr/bin/env python

"""The setup script."""

from collections import defaultdict
from setuptools import setup, find_packages
from itertools import chain
from pathlib import Path


def load_requirements(path: Path):
    requirements = defaultdict(list)
    with path.open("r") as fp:
        reqs_type = "base"
        for line in fp.readlines():
            if line.startswith("## extras:"):
                reqs_type = line.partition(":")[-1].strip()
                if reqs_type in ("base", "all"):
                    raise ValueError(f"'{reqs_type}' is a reserved extras keyword.")
            if line.startswith("-r"):
                requirements += load_requirements(line.split(" ")[1].strip())["base"]
            else:
                requirement = line.strip()
                if requirement and not requirement.startswith("#"):
                    requirements[reqs_type].append(requirement)
    return requirements


requirements = load_requirements(Path(__file__).parent / "requirements.txt")
extra_reqs = {k: v for k, v in requirements.items() if k != "base"}
extra_reqs["all"] = list(chain(*extra_reqs.values()))

readme = Path("README.md").read_text(encoding="UTF-8")

setup(
    author="DrivenData",
    author_email="info@drivendata.org",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description=("pathlib-style classes for cloud storage services"),
    extras_require=extra_reqs,
    install_requires=requirements["base"],
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    name="cloudpathlib",
    packages=find_packages(exclude=["tests"]),
    package_data={"cloudpathlib": ["py.typed"]},
    project_urls={
        "Bug Tracker": "https://github.com/drivendataorg/cloudpathlib/issues",
        "Documentation": "https://cloudpathlib.drivendata.org/",
        "Source Code": "https://github.com/drivendataorg/cloudpathlib",
    },
    url="https://github.com/drivendataorg/cloudpathlib",
    version="0.12.0",
)
