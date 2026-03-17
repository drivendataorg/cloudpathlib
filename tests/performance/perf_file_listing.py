def folder_list(folder, recursive):
    """Tests *Client._list_dir function and returns
    the number of items listed
    """
    return {"n_items": len(list(folder.client._list_dir(folder, recursive=recursive)))}


def glob(folder, recursive):
    if recursive:
        return {"n_items": len(list(folder.rglob("*.item")))}
    else:
        return {"n_items": len(list(folder.glob("*.item")))}


def glob_no_prefilter(folder, recursive):
    """Same patterns as glob but with server-side prefiltering disabled."""
    if recursive:
        return {"n_items": len(list(folder._glob("**/*.item", _prefilter=False)))}
    else:
        return {"n_items": len(list(folder._glob("*.item", _prefilter=False)))}


def walk(folder):
    n_items = 0

    for _, _, files in folder.walk():
        n_items += len(files)

    return {"n_items": n_items}


# ── Vanilla SDK baselines (no cloudpathlib overhead) ──


def _get_prefix(folder):
    prefix = getattr(folder, "blob", None) or getattr(folder, "key", "")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def vanilla_list(folder):
    """Raw SDK list_blobs / list_objects_v2 — just count objects."""
    prefix = _get_prefix(folder)
    cp = folder.cloud_prefix
    n = 0

    if cp == "gs://":
        for _ in folder.client.client.bucket(folder.bucket).list_blobs(prefix=prefix):
            n += 1
    elif cp == "s3://":
        for page in folder.client.client.get_paginator("list_objects_v2").paginate(
            Bucket=folder.bucket, Prefix=prefix
        ):
            n += len(page.get("Contents", []))
    elif cp == "az://":
        cc = folder.client.service_client.get_container_client(folder.container)
        for _ in cc.list_blobs(name_starts_with=prefix):
            n += 1

    return {"n_items": n}


def vanilla_glob(folder):
    """Raw SDK glob — match_glob on GCS, client-side fnmatch elsewhere."""
    import fnmatch

    prefix = _get_prefix(folder)
    cp = folder.cloud_prefix
    n = 0

    if cp == "gs://":
        bucket = folder.client.client.bucket(folder.bucket)
        for _ in bucket.list_blobs(prefix=prefix, match_glob=f"{prefix}**/*.item"):
            n += 1
    elif cp == "s3://":
        for page in folder.client.client.get_paginator("list_objects_v2").paginate(
            Bucket=folder.bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                if fnmatch.fnmatch(obj["Key"], "*.item"):
                    n += 1
    elif cp == "az://":
        cc = folder.client.service_client.get_container_client(folder.container)
        for blob in cc.list_blobs(name_starts_with=prefix):
            if fnmatch.fnmatch(blob.name, "*.item"):
                n += 1

    return {"n_items": n}
