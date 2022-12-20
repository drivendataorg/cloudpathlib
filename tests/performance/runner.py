import csv
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Callable, Dict, List

from statistics import mean, stdev


from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from loguru import logger

from cloudpathlib import CloudPath


from perf_file_listing import folder_list, glob


# make loguru and tqdm play nicely together
logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)


def construct_tree(folder_depth, sub_folders, items_per_folder):
    for limit in range(0, folder_depth + 1):
        if limit == 0:
            prefix = ""
        else:
            prefix = "/".join(f"level_{d:05}" for d in range(1, limit + 1))
            prefix += "/"  # append slash for when this gets combined later

        for f in range(0, sub_folders + 1):
            if f == 0:
                folder_prefix = prefix
            else:
                folder_prefix = f"{prefix}folder_{f:05}/"

            for i in range(1, items_per_folder + 1):
                yield f"{folder_prefix}{i:05}.item"


def setup_test(root, folder_depth, sub_folders, items_per_folder, overwrite=False):
    test_config_str = f"{folder_depth}_{sub_folders}_{items_per_folder}"
    test_folder = CloudPath(root) / test_config_str

    if test_folder.exists():
        if not overwrite:
            logger.info(
                f"Folder '{test_folder}' already exists, setup complete. Pass 'overwrite=True' to delete and rewrite"
            )
            return test_folder
        else:
            logger.info(
                f"Folder '{test_folder}' already exists, and overwrite=True. Removing existing folder."
            )
            test_folder.rmtree()

    logger.info(f"Setting up files for testing in folder: {test_folder}")

    # create folders and files for test in parallel
    thread_map(
        lambda x: (test_folder / x).touch(),
        list(construct_tree(folder_depth, sub_folders, items_per_folder)),
        desc="creating...",
    )

    return test_folder


@dataclass
class PerfRunConfig:
    name: str
    args: List
    kwargs: Dict


def run_single_perf_test(
    func: Callable, iterations: int, burn_in: int, configs: List[PerfRunConfig]
):
    all_results = {}
    for c in configs:
        measurements = []
        for i in tqdm(range(iterations + burn_in), desc=c.name):
            t0 = perf_counter()

            result = func(*c.args, **c.kwargs)

            if i >= burn_in:
                measurements.append(perf_counter() - t0)

        stats = {}
        stats["mean"] = mean(measurements)
        stats["max"] = max(measurements)
        stats["std"] = stdev(measurements)
        stats["iterations"] = iterations

        if isinstance(result, dict):
            stats.update(result)
        else:
            stats["result"] = result

        all_results[c.name] = stats  # add any return information from the test itself

    return all_results


def main(root, iterations, burn_in):
    # required folder setups; all totals over ~5,000 so that we get
    # automatically paginated by AWS S3 API
    shallow = setup_test(root, 0, 0, 5_500)
    normal = setup_test(root, 5, 100, 12)
    deep = setup_test(root, 50, 5, 25)

    test_suite = [
        (
            "List Folders",
            folder_list,
            [
                PerfRunConfig(name="List shallow recursive", args=[shallow, True], kwargs={}),
                PerfRunConfig(name="List shallow non-recursive", args=[shallow, False], kwargs={}),
                PerfRunConfig(name="List normal recursive", args=[normal, True], kwargs={}),
                PerfRunConfig(name="List normal non-recursive", args=[normal, False], kwargs={}),
                PerfRunConfig(name="List deep recursive", args=[deep, True], kwargs={}),
                PerfRunConfig(name="List deep non-recursive", args=[deep, False], kwargs={}),
            ],
        ),
        (
            "Glob scenarios",
            glob,
            [
                PerfRunConfig(name="Glob shallow recursive", args=[shallow, True], kwargs={}),
                PerfRunConfig(name="Glob shallow non-recursive", args=[shallow, False], kwargs={}),
                PerfRunConfig(name="Glob normal recursive", args=[normal, True], kwargs={}),
                PerfRunConfig(name="Glob normal non-recursive", args=[normal, False], kwargs={}),
                PerfRunConfig(name="Glob deep recursive", args=[deep, True], kwargs={}),
                PerfRunConfig(name="Glob deep non-recursive", args=[deep, False], kwargs={}),
            ],
        ),
    ]

    logger.info(
        f"Running performance test suite: root={root}, iter={iterations}, burn_in={burn_in}"
    )

    all_results = {}

    for name, func, confs in tqdm(test_suite, desc="Tests"):
        all_results[name] = run_single_perf_test(func, iterations, burn_in, confs)

    return all_results


def normalize_results(results):
    """Convert nested dict of results to a list of dicts with all the same keys."""
    rows_list = []
    observed_fields = set()

    for test_name, test_results in results.items():
        for conf_name, conf_results in test_results.items():
            row_dict = {"test_name": test_name, "config_name": conf_name}
            row_dict.update(conf_results)
            rows_list.append(row_dict)

            observed_fields |= set(row_dict.keys())

    # order fields for ones present in all tests; then any extra fields at the end
    common_fields = ["test_name", "config_name", "iterations", "mean", "std", "max"]

    all_fields = common_fields + list(observed_fields - set(common_fields))

    return all_fields, rows_list


def results_to_csv(results, path):
    """Save the results from `runner.main` to a csv file."""
    all_fields, row_list = normalize_results(results)

    with Path(path).open("w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fields)
        writer.writeheader()

        for row in row_list:
            # normalize so all rows have all possible fields
            for field in all_fields:
                if field not in row:
                    row[field] = None

            writer.writerow(row)
