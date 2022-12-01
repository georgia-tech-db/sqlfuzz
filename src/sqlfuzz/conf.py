#!/usr/bin/env python2
import os
import sys
from termcolor import colored

TARGETS = {}
TARGETS['postgres'] = 5432
TARGETS['mysql'] = 3306
TARGETS['cockroach'] = 26252
TARGETS['sqlite'] = None
TARGETS['sqlitel'] = None
PROB_TABLE = {}
DEFAULT_FUZZ_MAIN = "/tmp/fuzz"
TMP_DBMIN = "/tmp/dbmin/"

NOT_BUG              = 0
CORRECTNESS_BUG      = 1
LIMIT_FALSE_POSITIVE = 2
CRASH_BUG            = 3
PERFORMANCE_BUG      = 4

# for DB/QUERY minimization
TMP_QUERY_STORE = "/tmp/queries/"
TMP_REDUCTION_FILE = "reduction"
# for extracting query signature
SCALAR_STR = "scalar string"
SCALAR_INT = "4666"

LITERAL_COLUMN_STR = "literal col string"
LITERAL_COLUMN_INT = "2333"
class Logger (object):
    def __init__(self, target_db):
        self.logfile = "fuzz_log"
        
    def debug(self, msg, _print=False, _color='black'):
        with open(self.logfile, 'a') as f:
            f.write(msg+"\n")

        if _print:
            if _color== 'black':
                print(msg)
            else:
                print (colored(msg, _color))
