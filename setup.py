#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
from itertools import chain
from pathlib import Path


def load_requirements(path: Path):
    requirements = []
    with path.open("r") as fp:
        for line in fp.readlines():
            if line.startswith("-r"):
                requirements += load_requirements(line.split(" ")[1].strip())
            else:
                requirement = line.strip()
                if requirement and not requirement.startswith("#"):
                    requirements.append(requirement)
    return requirements


readme = Path("README.md").read_text(encoding="UTF-8")

extra_reqs = {}
for req_path in (Path(__file__).parent / "requirements").glob("*.txt"):
    if req_path.stem == "base":
        base_reqs = load_requirements(req_path)
        continue
    if req_path.stem == "all":
        raise ValueError("'all' is a reserved keyword and can't be used for a cloud provider key")
    extra_reqs[req_path.stem] = load_requirements(req_path)
extra_reqs["all"] = list(chain(*extra_reqs.values()))

setup(
    author="DrivenData",
    author_email="info@drivendata.org",
    python_requires=">=3.6",
    classifiers=[
        "Framework :: Jupyter",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description=(""),
    extras_require=extra_reqs,
    install_requires=base_reqs,
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    name="cloudpathlib",
    packages=find_packages(),
    project_urls={
        "Bug Tracker": "https://github.com/drivendataorg/cloudpathlib/issues",
        "Documentation": "https://cloudpathlib.drivendata.org/",
        "Source Code": "https://github.com/drivendataorg/cloudpathlib",
    },
    url="https://github.com/drivendataorg/cloudpathlib",
    version="0.2.0",
)
