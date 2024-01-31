from copy import copy
from pathlib import Path

import pandas as pd

import cloudpathlib


def print_table():
    path_base = {m for m in dir(Path) if not m.startswith("_")}

    lib_methods = {
        v.path_class.__name__: {m for m in dir(v.path_class) if not m.startswith("_")}
        for k, v in cloudpathlib.cloudpath.implementation_registry.items()
    }

    all_methods = copy(path_base)

    for _cls, methods in lib_methods.items():
        all_methods = all_methods.union(methods)

    df = pd.DataFrame(index=list(all_methods))
    df.index.name = "Methods + properties"

    for _cls, methods in lib_methods.items():
        df[f"`{_cls}`"] = [m in methods for m in df.index]

    # sort by bas
    df["base"] = [10 * (m in path_base) for m in df.index]

    df["sort_order"] = -df.sum(axis=1)

    df.index = df.index.to_series().apply(lambda x: f"`{x}`")

    md = (
        df.reset_index()
        .sort_values(
            ["sort_order", "Methods + properties"],
        )
        .set_index("Methods + properties")
        .drop(["sort_order", "base"], axis=1)
        .replace({True: "✅", False: "❌"})
        .to_markdown()
    )

    print(md)


if __name__ == "__main__":
    print_table()
