#!/usr/bin/env python3
# library
import sys
import random
from copy import deepcopy

# sqlfuzz
import sqlfuzz.randoms as randoms
import inspect
import sqlfuzz.builtin as builtin
from sqlfuzz.mutator_conf import MAX_STRING


def ret_funcs(funcname):
    ret_dict = {}
    if funcname == 'all':
        for name, obj in inspect.getmembers(sys.modules['builtin']):
            if inspect.isclass(obj):
                if name.startswith('sqlfunc_'):
                    ret_dict[name] = obj

    else:
        for name, obj in inspect.getmembers(sys.modules['builtin']):
            if inspect.isclass(obj):
                if name.startswith('sqlfunc_%s' % funcname):
                    ret_dict[name] = obj

    return ret_dict

def ret_funcs_str(f, gen_num):
    # f: sqlfunc class

    """
    1) common constraints
     - arg_length
     - input_type: in enum('num', 'str', 'any')
     - ret_type: in enum('num', 'str', 'bool', 'null', 'any')

    2) special constraints
     - like_constraint
     - value_min
     - value_max
    """

    # 1) decide the number of input values
    obj = f()
    arg_length = obj.arg_length
    input_type = obj.input_type

    if arg_length == builtin.UNLIMITED:
        arg_length = randoms.random_int_range(10)

    for _ in range(arg_length-1):
        input_type.append(input_type[0])

    inputs = []

    # 2) fill up the input with random values
    for x in range(arg_length):
        itype = input_type[x]
        sqltype = randoms.sqltype2pytype(itype)

        # generate random data
        # we should check attribute (min, max)
        # - if hasattr(a, 'property'):

        # when we want to change the default string size
        if randoms.prob(50):
            constraint=randoms.random_int_range(MAX_STRING)
        else:
            constraint=False

        # when we need to limit the min/max valuie
        if hasattr(obj, 'value_min'):
            inputdata = randoms.ret_randomdata_by_type(sqltype, constraint=constraint, min=obj.value_min, max=obj.value_max)
        else:
            inputdata = randoms.ret_randomdata_by_type(sqltype, constraint=constraint)
        inputs.append(inputdata)

    # special case: like condition
    if hasattr(obj, 'like_constraint'):
        # select like ('%c%', 'dcdd');
        inputs = []
        if randoms.prob(50):
            second_str = randoms.ret_randomdata_by_type('String')
        else:
            second_str = randoms.ret_randomdata_by_type('Unicode')

        left_length = int(len(second_str) * randoms.random_float_minmax(0.01, 0.5))
        right_length = int(len(second_str) * randoms.random_float_minmax(0.01, 0.5))

        #print(left_length, right_length)
        first_str = ''.join(list(second_str)[left_length:len(second_str) - right_length])
        #print (len(first_str), len(second_str))

        for x in range(randoms.random_int_range(len(first_str))):
            rand_index = randoms.random_int_range(len(first_str))
            tmp_list = list(first_str)
            tmp_list [rand_index-1] = '_'

        first_str = "%%%s%%" % (''.join(tmp_list))

        inputs.append(first_str)
        inputs.append(second_str)

    # TODO: special case for datetime and printf

    # 3) return the str
    ret_str = builtin.dump_to_str(obj.name, inputs)
    return ret_str


class SelectQueries(object):
    def __init__(self):
        pass

    def unit_test(self, test, gen_num, dry_run=False):
        """ test
        1) generation
        2) execution
         - we should not get error unless it is intended
        """

        functions = ret_funcs(test)
        #print (functions)
        for name,f in functions.items():
            #print (name)
            funcstr = ret_funcs_str(f, gen_num)
            select_statement = "SELECT %s;" % funcstr
            print (select_statement)

    def test(self):
        test = builtin.sqlfunc_abs()
        test.input = [1]
        print (test.toStr())

        test2 = builtin.sqlfunc_max()
        test2.input = [1,2.2,True, None,'a']
        print (test2.toStr())

        test3 = builtin.sqlfunc_likelihood()
        test3.input = [0.1, 0.5]
        print (test3.toStr())
        print (test3.value_max)
        print (test3.value_min)

        builtin.enum_functions()

"""
def enum_functions():

    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj):
            if name.startswith('func_'):
                print (name)
"""

""" AST test
 func1:
  - arg1: int, arg2: str
  - ret: int
 func2:
  - arg1: int, arg2: str
  - ret: str

 operator1:
  - '+'
  - binary (int, int)
  - ret (int)

 operator2:
  - 'IN'
  - binary (str, str)
  - ret (str)

 cast:
  - cast ( XX as 'str' or 'int')
  - ret str or int

"""

