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
