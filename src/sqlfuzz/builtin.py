import sys
import inspect
import itertools

#from pprint import pprint

# ret_type   = enum('num', 'str', 'bool', 'null', 'any')
# input_type = enum('num', 'str', 'any', 'null')

UNLIMITED = 100

def dump_to_str(funcname, argv):        

    temp_list = []
    for arg in argv:
        #print(arg, type(arg))
        if isinstance(arg, int) or isinstance(arg, float):
            temp_list.append(str(arg))
        elif isinstance(arg, type(None)):
            temp_list.append("NULL")
        elif isinstance(arg, str):
            temp_list.append("\'%s\'" % arg)
        elif isinstance(arg, bool):
            if arg == True:
                temp_list.append("TRUE")
            else:
                temp_list.append("FALSE")
    return "%s (%s)" % (funcname, ', '.join(temp_list))

def enum_functions():    
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj):            
            if name.startswith('sqlfunc_'):
                #print (name)
                #print (obj.return_type)
                pass
    
class Func(object):
    def __init__(self): 
        self.input = None
        self.name = str.upper(self.__class__.__name__.split("_")[1])        
           
    @property
    def return_type(self):
        raise NotImplementedError

    @property
    def input_type(self):
        raise NotImplementedError
    
    def toStr(self):
        dumpstr = dump_to_str(str.upper(self.name), self.input)
        return self.name + dumpstr


class sqlfunc_abs(Func):
    def __init__(self):
        super(sqlfunc_abs, self).__init__()
        self.arg_length = 1
        self.default = [1]
    
    @property
    def return_type(self):
        return 'num'

    @property
    def input_type(self):
        return ['num']


class sqlfunc_char(Func):
    def __init__(self, *argv):
        super(sqlfunc_char, self).__init__()
        self.arg_length = UNLIMITED
        self.value_min = 0.0
        self.value_max = 500.0
        self.default = [65,66,67]
    
    @property
    def return_type(self):
        return 'str'

    @property
    def input_type(self):
        return ['num']


class sqlfunc_coalesce(Func):
    def __init__(self, *argv):
        super(sqlfunc_coalesce, self).__init__()
        self.arg_length = UNLIMITED
    
    @property
    def return_type(self):
        return 'any'

    @property
    def input_type(self):
        return ['any']


class sqlfunc_glob(Func):
    def __init__(self, *argv):
        super(sqlfunc_glob, self).__init__()
        self.arg_length = 2
    
    @property
    def return_type(self):
        return 'num'

    @property
    def input_type(self):
        return ['any', 'any']


class sqlfunc_hex(Func):
    def __init__(self, *argv):
        super(sqlfunc_hex, self).__init__()
        self.arg_length = 1
    
    @property
    def return_type(self):
        return 'num'

    @property
    def input_type(self):
        return ['str']


class sqlfunc_ifnull(Func):
    def __init__(self, *argv):
        super(sqlfunc_ifnull, self).__init__()
        self.arg_length = 2

    @property
    def input_type(self):
        return ['any']
    
    @property
    def return_type(self):
        return 'any'


class sqlfunc_instr(Func):
    def __init__(self, *argv):
        super(sqlfunc_instr, self).__init__()
        self.arg_length = 2

    @property
    def input_type(self):
        return ['str', 'str']
    
    @property
    def return_type(self):
        return 'num'


class sqlfunc_like(Func):
    def __init__(self, *argv):
        super(sqlfunc_like, self).__init__()
        self.arg_length = 2
        self.like_constraint = True

    @property
    def input_type(self):        
        return ['str', 'str']
    
    @property
    def return_type(self):
        return 'bool'


class sqlfunc_likelihood(Func):
    def __init__(self, *argv):
        super(sqlfunc_likelihood, self).__init__()
        self.arg_length = 2
        self.value_min = 0.0
        self.value_max = 1.0

    @property
    def input_type(self):        
        return ['num', 'num']
    
    @property
    def return_type(self):
        return 'num'


class sqlfunc_likely(Func):
    def __init__(self, *argv):
        super(sqlfunc_likely, self).__init__()
        self.arg_length = 1
        self.value_min = 0.0
        self.value_max = 1.0

    @property
    def input_type(self):        
        return ['num']
    
    @property
    def return_type(self):
        return 'num'


class sqlfunc_lower(Func):
    def __init__(self, *argv):
        super(sqlfunc_lower, self).__init__()
        self.arg_length = 1        

    @property
    def input_type(self):        
        return ['str']
    
    @property
    def return_type(self):
        return 'str'

class sqlfunc_length(Func):
    def __init__(self, *argv):
        super(sqlfunc_length, self).__init__()
        self.arg_length = 1        

    @property
    def input_type(self):        
        return ['any']
    
    @property
    def return_type(self):
        return 'num'


class sqlfunc_max(Func):
    def __init__(self, *argv):
        super(sqlfunc_max, self).__init__()
        self.arg_length = UNLIMITED

    @property
    def return_type(self):
        return 'any'

    @property
    def input_type(self):
        return ['any']


class sqlfunc_min(Func):
    def __init__(self, *argv):
        super(sqlfunc_min, self).__init__()
        self.arg_length = UNLIMITED

    @property
    def return_type(self):
        return 'any'

    @property
    def input_type(self):
        return ['any']

## TODO: add rest of functions

