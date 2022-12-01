import time
import string
import random
import datetime
from datetime import date, timedelta
from sqlfuzz.mutator_conf import BOUNDARY, MAX_STRING


def random_unicode(length):

    try:
        get_char = unichr
    except NameError:
        get_char = chr

    # Update this to include code point ranges to be sampled
    include_ranges = [
        (0x0021, 0x0021),
        (0x0023, 0x0026),
        (0x0028, 0x007E),
        (0x00A1, 0x00AC),
        (0x00AE, 0x00FF),
        (0x0100, 0x017F),
        (0x0180, 0x024F),
        (0x2C60, 0x2C7F),
        (0x16A0, 0x16F0),
        (0x0370, 0x0377),
        (0x037A, 0x037E),
        (0x0384, 0x038A),
        (0x038C, 0x038C),
    ]

    alphabet = [
        get_char(code_point) for current_range in include_ranges
        for code_point in range(current_range[0], current_range[1] + 1)
    ]
    return ''.join(random.choice(alphabet) for i in range(length))


def strTimeProp(start, end, format, prop):
    #print(datetime.strptime(BOUNDARY["DateTime"][0], format))
    stime = time.mktime(time.strptime(BOUNDARY["DateTime"][0], format))
    etime = time.mktime(time.strptime(BOUNDARY["DateTime"][1], format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(format, time.localtime(ptime))


def random_date(start, end):
    #prop = random.random()
    # return strTimeProp(start, end, '%m/%d/%Y %I:%M %p', prop)
    start_date = datetime.datetime(1990, 1, 1)
    end_date = datetime.datetime(2020, 1, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)


def random_int_range(N):
    return random.randint(1, N)


def random_int_range_contain_zero(N):
    return random.randint(0, N)


def random_float_minmax(min, max):
    return random.uniform(min, max)


def random_int_minmax(min, max):
    return random.randint(min, max)


def random_digits(N):
    return ''.join(random.choice(string.digits) for _ in range(N))


def random_strings(N):
    return ''.join(random.choice(string.ascii_letters) for _ in range(N))


def random_character(N):
    return ''.join(random.choice(string.ascii_letters+string.digits + string.punctuation+'\t\n ') for _ in range(N))


def random_boolean(as_str=True):
    if prob(50):
        if as_str:
            return "TRUE"
        else:
            return True
    if as_str:
        return "FALSE"
    else:
        return False


def prob(N):
    if random_int_range(100) < N:
        return True
    return False


def sqltype2pytype(sqltype):
    """probability: select type,
     - for example, number could be Integer or Float
    """
    #input_type = enum('num', 'str', 'any')
    R = random.randrange(100)

    if sqltype == 'num':
        if R < 50:
            return 'Integer'
        else:
            return 'Float'

    elif sqltype == 'str':
        return 'String'

    elif sqltype == 'any':
        if R < 15:
            return 'String'
        elif R < 30:
            return 'Unicode'
        elif R < 45:
            return 'Integer'
        elif R < 60:
            return 'Float'
        elif R < 75:
            return 'DateTime'
        elif R < 100:
            return 'NULL'


def ret_randomdata_by_type(column_type, constraint=False, min=None, max=None):

    if column_type == "String":
        if constraint == False:
            return random_character(MAX_STRING)
        else:
            return random_character(constraint)

    elif column_type == "Unicode":
        if constraint == False:
            return random_unicode(MAX_STRING)
        else:
            return random_unicode(constraint)

    elif column_type == "Integer":
        if min == None:
            return random_int_minmax(BOUNDARY[column_type][0], BOUNDARY[column_type][1])
        else:
            return random_int_minmax(min, max)

    elif column_type == "Float":
        if min == None:
            return random_float_minmax(BOUNDARY[column_type][0], BOUNDARY[column_type][1])
        else:
            return random_float_minmax(min, max)

    elif column_type == "DateTime":
        # min=None
        # if min==None:
        return random_date(BOUNDARY[column_type][0], BOUNDARY[column_type][1])
        """
        else:
            #print (type(min), type(max))
            #print(min) #1900-01-01 13:30:00
            #print(max)
            return random_date(min, max)
        """

    elif column_type == "Boolean":
        return random_boolean(as_str=True)

    elif column_type == "NULL":
        return None

# /*
# index, default value, affected keyword,
#  0, 500, "or" / "and"
#  1, 500, "not" / ""
#  2, 500, "true" / "false"
#  3, 11, "distinct" / ""
#  4, 668, "limit" / ""
#  5, 110, "case"
#  6, 24, "nullif"
#  7, 24, "coalesce"
#  8, 84, atomic_subselect (i.e., one line select statement with one row)
#  9, 500, subquery
# 10, 500, join
# 11, 500, join - inner
# 12, 500, join - left
# 13, 500, join - right
# 14, 0,   reserved  - reserved
# */
probability = [500, 500, 500,  11, 668,
               110,  24,  24,  84, 500,
               500, 500, 500, 500,   0]
