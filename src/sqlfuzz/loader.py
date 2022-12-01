import os, sys
import sqlfuzz.conf as conf
import json
def load_pbtable(filename):
    with open(filename) as f:
        print("Try read probability table", f.name)
        conf.PROB_TABLE = json.load(f)
        print(conf.PROB_TABLE)
        if (conf.PROB_TABLE is None):
            raise ValueError ("Loaded PROB_TABLE is none")

    return conf.PROB_TABLE
