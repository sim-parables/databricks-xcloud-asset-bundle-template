"""Local Spark Run

Use this file for running Spark Jobs locally for faster debugging and troubleshooting.

Change the task import in the code below to trigger tasks as required.
"""
import argparse
import importlib.util
import sys

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_entrypoint', help='Pytest custom entrypoint module', type=str)

    return parser.parse_known_args()

def get_task(file_path):
    spec = importlib.util.spec_from_file_location('entrypoint', file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["entrypoint"] = module
    spec.loader.exec_module(module)

    return module.entrypoint

if __name__ == '__main__':
    args, unknown_args = _parse_args()
    entrypoint = get_task(args.test_entrypoint)
    entrypoint()