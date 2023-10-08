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


def walk(folder):
    n_items = 0

    for _, _, files in folder.walk():
        n_items += len(files)

    return {"n_items": n_items}
