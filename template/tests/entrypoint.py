from pytest_jsonreport.plugin import JSONReport

import argparse
import pytest
import sys
import os

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_entrypoint', help='Pytest custom entrypoint module', type=str)
    parser.add_argument('--test_path', help='Pytest script path', type=str)
    parser.add_argument('--csv_path', help="CSV Path", type= str)
    parser.add_argument('--output_path', help="Cloud Storage Path", type= str)
    parser.add_argument('--output_table', help="Unity Catalog Table", type= str)
    args, unknown_args = parser.parse_known_args()
    if args.test_path:
        return [args.test_path]
    
    return unknown_args

def _evaluate_tests(report):
    fails = []
    if report and 'tests' in report:
        for t in report['tests']:
            if t['outcome'] == 'failed':
                fails.append({
                    'nodeid': t['nodeid'], 
                    'details': t['call']['crash']
                })
    
    if len(fails) > 0:
        raise Exception(fails)

def entrypoint(args=_parse_args()):
    sys.dont_write_bytecode = True
    os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
    
    plugin = JSONReport()
    pytest.main(args, plugins=[plugin])

    _evaluate_tests(plugin.report)

if __name__ == '__main__':
    entrypoint()