#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
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


readme = Path("README.md").read_text()

requirements = load_requirements(Path(__file__).parent / "requirements.txt")

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
    description=(
        ""
    ),
    install_requires=requirements,
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
)