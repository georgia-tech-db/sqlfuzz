#!/usr/bin/env python2
#COLUMN_TYPES = ['BigInteger', 'Boolean', 'Date', 'DateTime', 'Float'\
#    'Integer', 'Numeric', 'SmallInteger', 'String', 'Text', 'Time']

from datetime import datetime
#from sqlalchemy import create_engine, Table, Column, SmallInteger, Numeric, Float, Integer, String, BigInteger, Boolean, Date, DateTime, MetaData, ForeignKey, Text, Time
from sqlalchemy import create_engine, Table, Column, String, MetaData, ForeignKey, DateTime, Float, Integer

# varibales
FUZZ_MAIN = "test_mutator"
DB_FILE = "test.db"
TMP_QUERY = "/tmp/query"

# List of any number types
NUMERIC = ['SmallInteger', 'Numeric', 'Float', 'Integer', 'BigInteger', 'Boolean']

### Define constraints for the type ###
BOUNDARY = {}

# Integer
BOUNDARY["Integer"]  = (-9223372036854775808, 9223372036854775808)

# DateTime
BOUNDARY["DateTime"] = ('1/1/1900 1:30 PM', '1/1/2100 4:50 AM')
DATE_START = datetime.strptime(BOUNDARY["DateTime"][0], '%m/%d/%Y %I:%M %p')
DATE_END   = datetime.strptime(BOUNDARY["DateTime"][1], '%m/%d/%Y %I:%M %p')
#BOUNDARY["DateTime"] = (DATE_START, DATE_END)

# Float
BOUNDARY["Float"]    = (-9223372036854775808, 9223372036854775808)

# String
MAX_STRING = 20

# column configuration
COLUMN_TYPES = [Integer, String(MAX_STRING), DateTime, Float]
NUM_COLUMN_TYPES = len(COLUMN_TYPES)

# Operators
#OPERATORS = ["||", "*", "/", "%", "+", "-", "<<", ">>", "&", "|", "AND", "OR"]
COMPARISONS = ["<", "<=", ">", ">=", "=", "==", "<>", "IS", "IS NOT", \
    "IN", "LIKE", "GLOB", "MATCH", "REGEXP"]
SET_OPERATION = ["intersect", "intersect_all", "union", "union_all", "except_", "except_all"]
### Probabilities
# This can be used as feedback
PROB_TABLE = {}
PROB_TABLE["PROB_CREATE_INDEX"]   = 100 # % of index creation on create table statement
PROB_TABLE["PROB_REINDEX"]        = 100 # % of creating reindex clause on created one
PROB_TABLE["PROB_DROP_INDEX"]     = 100 # % of dropping among created index
PROB_TABLE["PROB_UNICODE"]        = 5  # % of string will be unicode
PROB_TABLE["PROB_SHARED_DATA"]    = 90 # % of foreign key data (same as referencing table)
PROB_TABLE["PROB_UPDATE"]         = 90 # % of table will be affected by update query
PROB_TABLE["PROB_UPDATE_LIMIT"]   = 20 # % of updated table with limit clause
PROB_TABLE["PROB_DELETE"]         = 90 # % of table will be affected by delete query
PROB_TABLE["PROB_DELETE_LIMIT"]   = 20
PROB_TABLE["PROB_CREATE_IF_NOT_EXIST"] = 100 # CREATE TABLE ==> CREATE TABLE IF NOT EXISTS

### templates
UPDATE = """
UPDATE {table}
SET {set}
WHERE {where}"""

DELETE = """
DELETE FROM {table}
WHERE {where}"""