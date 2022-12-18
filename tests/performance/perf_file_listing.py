def folder_list(folder, recursive):
    """Tests *Client._list_dir function and returns
    the number of items listed
    """
    return {"n_items": len(list(folder.client._list_dir(folder, recursive=recursive)))}
