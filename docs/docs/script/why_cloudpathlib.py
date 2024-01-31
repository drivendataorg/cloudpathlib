#!/usr/bin/env python
# coding: utf-8

# # Why cloudpathlib?

# ## We 😍 pathlib
# 
# `pathlib` a wonderful tool for working with filesystem paths, available from the Python 3 standard library. 

from pathlib import Path


# For example, we can easily list all the files in a directory.

list(Path(".").glob("*"))


# There are methods to quickly learn everything there is to know about a filesystem path, and even do simple file manipulations.

notebook = Path("why_cloudpathlib.ipynb").resolve()

print(f"{'Path:':15}{notebook}")
print(f"{'Name:':15}{notebook.name}")
print(f"{'Stem:':15}{notebook.stem}")
print(f"{'Suffix:':15}{notebook.suffix}")
print(f"{'With suffix:':15}{notebook.with_suffix('.cpp')}")
print(f"{'Parent:':15}{notebook.parent}")
print(f"{'Read_text:'}\n{notebook.read_text()[:200]}\n")


# If you're new to pathlib, we highly recommend it over the older `os.path` module. We find that it has a much more intuitive and convenient interface. The [official documentation](https://docs.python.org/3/library/pathlib.html) is a helpful reference, and we also recommend this [excellent cheat sheet by Chris Moffitt](https://github.com/chris1610/pbpython/blob/master/extras/Pathlib-Cheatsheet.pdf). 

# ### Cross-platform support
# 
# One great feature about using `pathlib` over regular strings is that it lets you write code with cross-platform file paths. It "just works" on Windows too. Write path manipulations that can run on anyone's machine!
# 
# ```python
# path = Path.home()
# path
# >>> C:\Users\DrivenData\
# 
# docs = path / 'Documents'
# docs
# >>> C:\Users\DrivenData\Documents
# ```

# ## We also 😍 cloud storage
# 
# This is great, but I live in the future. Not every file I care about is on my machine. What do I do when I am working on S3? Do I have to explicitly download every file before I can do things with them?
# 
# **Of course not, if you use cloudpathlib!**

# load environment variables from .env file;
# not required, just where we keep our creds
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


from cloudpathlib import S3Path

s3p = S3Path("s3://cloudpathlib-test-bucket/why_cloudpathlib/file.txt")
s3p.name


# Nothing there yet...
s3p.exists()


# Touch (just like with `pathlib.Path`)
s3p.touch()


# Bingo!
s3p.exists()


# list all the files in the directory
[p for p in s3p.parent.iterdir()]


stat = s3p.stat()
print(f"File size in bytes: {stat.st_size}")
stat


s3p.write_text("Hello to all of my friends!")


stat = s3p.stat()
print(f"File size in bytes: {stat.st_size}")
stat


# Delete (again just like with `pathlib.Path`)
s3p.unlink()


s3p.exists()


# ### Cross-cloud support
# 
# That's cool, but I use Azure Blob Storage―what can I do?

from cloudpathlib import AzureBlobPath

azp = AzureBlobPath("az://cloudpathlib-test-container/file.txt")
azp.name


azp.exists()


azp.write_text("I'm on Azure, boss.")


azp.exists()


# list all the files in the directory
[p for p in azp.parent.iterdir()]


azp.exists()


azp.unlink()


# ### Cloud hopping
# 
# Moving between cloud storage providers should be a simple as moving between disks on your computer. Let's say that the Senior Vice President of Tomfoolery comes to me and says, "We've got a mandate to migrate our application to Azure Blob Storage from S3!"
# 
# No problem, if I used `cloudpathlib`! The `CloudPath` class constructor automatically dispatches to the appropriate concrete class, the same way that `pathlib.Path` does for different operating systems. 

from cloudpathlib import CloudPath

cloud_directory = CloudPath("s3://cloudpathlib-test-bucket/why_cloudpathlib/")

upload = cloud_directory / "user_upload.txt"
upload.write_text("A user made this file!")

assert upload.exists()
upload.unlink()
assert not upload.exists()


from cloudpathlib import CloudPath

# Changing this root path is the ONLY change!
cloud_directory = CloudPath("az://cloudpathlib-test-container/why_cloudpathlib/")

upload = cloud_directory / "user_upload.txt"
upload.write_text("A user made this file!")

assert upload.exists()
upload.unlink()
assert not upload.exists()

