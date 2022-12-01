import random
import sqlfuzz.randoms as randoms
import sqlfuzz.conf as conf
import sqlfuzz.loader as loader
from string import ascii_lowercase
from sqlfuzz.common import *
from sqlalchemy import or_, and_, cast, Float
from sqlalchemy import not_
from sqlalchemy import true,false
from sqlalchemy import type_coerce
from sqlalchemy import case,literal_column
from fractions import Fraction



def ret_columnlist(target):
    retlist = []
    for c in target:
        retlist.append(str(c))
    return retlist


def select_word(src_data, type):
    """
    type: start or end
    """
    oneword = random.choice(src_data)
    # if it is a single word, just return itself
    if len(oneword) ==1:
        return oneword
    offset = randoms.random_int_range(len(oneword) - 1)
    length = randoms.random_int_range(len(oneword) - offset)

    if type == "start":
        return oneword[:offset]
    elif type == "end":
        return oneword[-offset:]
    elif type == "ilike":
        return oneword.upper()
    elif type == "contain":
        return oneword[offset:offset + length]
    else:
        return oneword


def select_number(src_stat, src_data, type):
    if type == "op":
        return randoms.random_int_range(100)
    elif type == "comp":
        random_num = random.choice(src_data)
        stat = src_stat
        stat.append(random_num)
        return random.choice(stat)


def combine_condition(cond1, cond2, type):
    if type == "and":
        return cond1.__and__(cond2)
    elif type == "or":
        return cond1.__or__(cond2)


def combine_parenthesis(cond1, cond2, type):
    if type == "and":
        return and_(cond1, cond2)
    elif type == "or":
        return or_(cond1, cond2)

def join_cond_generator(col1, col2):
    srctype = str(col1.type)
    if "FLOAT" in srctype or "INT" in srctype:
        op_select = random.choice(OP)
        op_num = random.randint(0,1000)
        candidate = [
            col1.__eq__(col2),
            col1.__le__(col2),
            col1.__lt__(col2),
            col1.__ne__(col2),
            col1.__gt__(col2),
            col1.__ge__(col2),
            col1.op(op_select)(op_num).__eq__(col2),
            col1.op(op_select)(op_num).__le__(col2),
            col1.op(op_select)(op_num).__lt__(col2),
            col1.op(op_select)(op_num).__ne__(col2),
            col1.op(op_select)(op_num).__gt__(col2),
            col1.op(op_select)(op_num).__ge__(col2)]

        return random.choice(candidate)





# https://cassiopeia.readthedocs.io/en/v0.1.1/_modules/sqlalchemy/sql/operators.html
def where_generator(src_col, dst_col, src_stat, dst_stat, src_data):
    """
    input:  columns (left and/or right operand),
    return: where clause
    """

    # 1) column and value
    srctype = str(src_col.type)
    print("choose column for predicate generation!",src_col, type(src_col), srctype)
    if src_data is None:
        # if src data is none, create a random data
        if "CHAR" in srctype:
            src_data = ["".join(random.choices(ascii_lowercase, k=random.randint(1,10))) for _ in range(10)]

        elif "FLOAT" in srctype or "INT" in srctype:
            src_data = random.sample(range(1, 1000), 20)
            src_stat = random.sample(range(1, 1000), 20)

        else:
            print("unsupported src col", src_col)
            return None
        print("random srcdata is", src_data)



    """
    if dst_col is not None:
        dsttype = str(dst_col.type)
    else:
        dsttype = None
    """
    # TODO: more variation in predicate
    # random_idx_list = list(range(3))
    # random.shuffle(random_idx_list)
    # # 0 denotes arithmatic, 1 denotes function, 3 denotes cast
    # for idx in random_idx_list:
    #     if idx == 0:
    #         # perform an arithematic operation (number, date)

    if "CHAR" in srctype:
        startword = select_word(src_data, type="start")
        endword = select_word(src_data, type="end")
        ilikeword = select_word(src_data, type="ilike")
        randomword = select_word(src_data, type="random")
        print("random word is", randomword, src_col)
        containword = select_word(src_data, type="contain")
        candidate = [
            src_col.notlike(randomword),
            src_col.like(randomword),
            src_col.notlike(randomword),
            src_col.contains(containword),
            src_col.startswith(startword),
            src_col.endswith(endword),
            src_col.is_distinct_from(randomword),
            not_(src_col.is_distinct_from(randomword))]
            # src_col.notin_(randomword),
            # src_col.in_(randomword)]
        # # additional candidate case for string type
        # case_map={}
        # case_map[randomword] = "w"
        # case_clause = case(case_map,value=src_col)

        # candidate.append(case_clause)

        return random.choice(candidate)

    elif "FLOAT" in srctype or "INT" in srctype:
        if randoms.random_int_range(1000) < 900:
            op_num = select_number(src_stat, src_data, type="op")
        else:
            op_num_1 = select_number(src_stat, src_data, type="op")
            # denominator must larger than 0
            op_num_2 = select_number(src_stat, src_data, type="op")+ 1
            op_num = Fraction(op_num_1, op_num_2)

        op_select = random.choice(OP)
        comp_num = select_number(src_stat, src_data, type="comp")
        # return cast((src_col + Fraction(10, 18)), Integer).__eq__(comp_num)
        candidate = [
            src_col.__eq__(comp_num),
            src_col.__le__(comp_num),
            src_col.__lt__(comp_num),
            src_col.__gt__(comp_num),
            src_col.__ge__(comp_num),
            src_col.op(op_select)(op_num).__eq__(comp_num),
            src_col.op(op_select)(op_num).__le__(comp_num),
            src_col.op(op_select)(op_num).__lt__(comp_num),
            src_col.op(op_select)(op_num).__ne__(comp_num),
            src_col.op(op_select)(op_num).__gt__(comp_num),
            src_col.op(op_select)(op_num).__ge__(comp_num)]
        # print("comp or op", (random.choice(candidate).type))
        if randoms.random_int_range(1000) < 100:
            # cast the result
            cast2type = random.choice(CAST_TO_TYPE)
            return cast(random.choice(candidate), cast2type)
        else:
            return random.choice(candidate)


    elif "DATE" in srctype or "TIME" in srctype:
        # random_date = datetime.now()
        random_date = random.choice(src_data)
        src_year = [i.year for i in src_data]
        src_month = [i.month for i in src_data]


        op_select = random.choice(OP)
        op_num = select_number(src_stat, src_data, type="op")
        if (randoms.random_int_range(1000) < conf.PROB_TABLE["extractyear"]):
            print("extract date")
            random_year = random.choice(src_year)
            random_month = random.choice(src_month)

            candidate_year = [
            func.year(src_col).__eq__(random_year),
            func.year(src_col).__le__(random_year),
            func.year(src_col).__lt__(random_year),
            func.year(src_col).__ne__(random_year),
            func.year(src_col).__gt__(random_year),
            func.year(src_col).__ge__(random_year),
            func.year(src_col).op(op_select)(op_num).__eq__(random_year),
            func.year(src_col).op(op_select)(op_num).__le__(random_year),
            func.year(src_col).op(op_select)(op_num).__lt__(random_year),
            func.year(src_col).op(op_select)(op_num).__ne__(random_year),
            func.year(src_col).op(op_select)(op_num).__gt__(random_year),
            func.year(src_col).op(op_select)(op_num).__ge__(random_year)]
            candidate_month = [
            func.month(src_col).__eq__(random_month),
            func.month(src_col).__le__(random_month),
            func.month(src_col).__lt__(random_month),
            func.month(src_col).__ne__(random_month),
            func.month(src_col).__gt__(random_month),
            func.month(src_col).__ge__(random_month),
            func.month(src_col).op(op_select)(op_num).__eq__(random_month),
            func.month(src_col).op(op_select)(op_num).__le__(random_month),
            func.month(src_col).op(op_select)(op_num).__lt__(random_month),
            func.month(src_col).op(op_select)(op_num).__ne__(random_month),
            func.month(src_col).op(op_select)(op_num).__gt__(random_month),
            func.month(src_col).op(op_select)(op_num).__ge__(random_month)]
            if (randoms.random_int_range(1000) < conf.PROB_TABLE["extractmonth"]):
                combine_condition(random.choice(candidate_year), random.choice(candidate_month),
                                                   random.choice(CONJ))
            else:
                return random.choice(candidate_year)



        candidate = [
            src_col.__eq__(random_date),
            src_col.__le__(random_date),
            src_col.__lt__(random_date),
            src_col.__ne__(random_date),
            src_col.__gt__(random_date),
            src_col.__ge__(random_date),
            src_col.op(op_select)(op_num).__eq__(random_date),
            src_col.op(op_select)(op_num).__le__(random_date),
            src_col.op(op_select)(op_num).__lt__(random_date),
            src_col.op(op_select)(op_num).__ne__(random_date),
            src_col.op(op_select)(op_num).__gt__(random_date),
            src_col.op(op_select)(op_num).__ge__(random_date)]
            # .filter(extract('year', Foo.Date) == 2012)

        return random.choice(candidate)

    # 2) column and column comparison
    # 2-1) same type columns
    # 2-2) cast src or dst column


def mutate_add_casting(cond):
    return cond


def mutate_cond_column(cond, src_col, dst_col, dst_stat, dst_data, direction):
    """
    Change column, operator, value of the BinaryExpression
    (we are sending auxilary information for the better result)
    """

    srctype = str(dst_col.type)

    if "CHAR" in srctype:
        startword = select_word(dst_data, type="start")
        endword = select_word(dst_data, type="end")
        ilikeword = select_word(dst_data, type="ilike")
        randomword = select_word(dst_data, type="random")
        containword = select_word(dst_data, type="contain")

        candidate = [
            dst_col.notlike(randomword),
            dst_col.like(randomword),
            dst_col.notlike(randomword),
            dst_col.ilike(ilikeword),
            dst_col.notilike(ilikeword),
            dst_col.contains(containword),
            dst_col.concat(-1.33),
            dst_col.startswith(startword),
            dst_col.endswith(endword),
            dst_col.collate('NOCASE'),
            dst_col.isnot(randomword),
            dst_col.is_(randomword),
            dst_col.notin_(randomword),
            dst_col.in_(randomword)]

        chosen = random.choice(candidate)

    elif "FLOAT" in srctype or "INT" in srctype:

        op_num = select_number(dst_stat, dst_data, type="op")
        op_select = random.choice(OP)
        comp_num = select_number(dst_stat, dst_data, type="comp")

        candidate = [
            dst_col.__eq__(comp_num),
            dst_col.__le__(comp_num),
            dst_col.__lt__(comp_num),
            dst_col.__ne__(comp_num),
            dst_col.__gt__(comp_num),
            dst_col.__ge__(comp_num),
            dst_col.op(op_select)(op_num).__eq__(comp_num),
            dst_col.op(op_select)(op_num).__le__(comp_num),
            dst_col.op(op_select)(op_num).__lt__(comp_num),
            dst_col.op(op_select)(op_num).__ne__(comp_num),
            dst_col.op(op_select)(op_num).__gt__(comp_num),
            dst_col.op(op_select)(op_num).__ge__(comp_num)]

        chosen = random.choice(candidate)

    elif "DATE" in srctype or "TIME" in srctype:
        random_date = random.choice(dst_data)
        op_select = random.choice(OP)
        op_num = select_number(dst_stat, dst_data, type="op")

        candidate = [
            dst_col.__eq__(random_date),
            dst_col.__le__(random_date),
            dst_col.__lt__(random_date),
            dst_col.__ne__(random_date),
            dst_col.__gt__(random_date),
            dst_col.__ge__(random_date),
            dst_col.op(op_select)(op_num).__eq__(random_date),
            dst_col.op(op_select)(op_num).__le__(random_date),
            dst_col.op(op_select)(op_num).__lt__(random_date),
            dst_col.op(op_select)(op_num).__ne__(random_date),
            dst_col.op(op_select)(op_num).__gt__(random_date),
            dst_col.op(op_select)(op_num).__ge__(random_date)]

        chosen = random.choice(candidate)

    # TODO: prevent error using the above information
    if direction == "left":
        cond.left = dst_col
    elif direction == "right":
        cond.right = dst_col

    return cond


def _mutate_value(bind_param, TF=None):
    # print(bind_param.value)
    out = [True, False]
    srctype = str(bind_param.type)

    if "CHAR" in srctype:
        bind_param.value = randoms.ret_randomdata_by_type("String")
    elif "FLOAT" in srctype:
        bind_param.value = randoms.ret_randomdata_by_type("Float")
        if TF is not None:
            bind_param.value = random.choice(out)
    elif "INT" in srctype:
        bind_param.value = randoms.ret_randomdata_by_type("Integer")
        if TF is not None:
            bind_param.value = random.choice(out)
    elif "DATE" in srctype or "TIME" in srctype:
        bind_param.value = randoms.ret_randomdata_by_type("DateTime")

    return bind_param


def mutate_cond_value(cond, TF=None):
    """
    Mutate value in the condition
    """
    if "BindParameter" in type(cond.left).__name__:
        cond.left = _mutate_value(cond.left, TF)
    elif "BindParameter" in type(cond.right).__name__:
        cond.right = _mutate_value(cond.right, TF)
    return cond


def mutate_func(cond_left, dst_col):

    candidate = [func.max(dst_col),
                 func.min(dst_col),
                 func.avg(dst_col),
                 func.abs(dst_col),
                 func.count(dst_col)]
    cond_left = random.choice(candidate)
    return cond_left


def cast_generator(src_col, dst_col, src_stat, dst_stat, src_data):
    """
    input:  columns (left and/or right operand),
    return: column with cast condition
     - cast(a as numeric) > 1
    """

    # 1) column and value
    curtype = str(src_col.type)

    # 2) select casted datatype
    casttype = [Float, Integer, String, DateTime]
    srctype = random.choice(casttype)


    # 3) made the casting
    if "String" in srctype.__name__:
        # startword = select_word(src_data, type="start")
        # endword = select_word(src_data, type="end")
        # ilikeword = select_word(src_data, type="ilike")
        randomword = select_word(src_data, type="random")
        # containword = select_word(src_data, type="contain")

        op_num = select_number(src_stat, src_data, type="op")
        op_select = random.choice(OP)

        candidate = [
            src_col.__eq__(randomword),
            src_col.__le__(randomword),
            src_col.__lt__(randomword),
            src_col.__ne__(randomword),
            src_col.__gt__(randomword),
            src_col.__ge__(randomword),
            src_col.op(op_select)(op_num).__eq__(randomword),
            src_col.op(op_select)(op_num).__le__(randomword),
            src_col.op(op_select)(op_num).__lt__(randomword),
            src_col.op(op_select)(op_num).__ne__(randomword),
            src_col.op(op_select)(op_num).__gt__(randomword),
            src_col.op(op_select)(op_num).__ge__(randomword)]

        return random.choice(candidate)

    elif "Float" in srctype.__name__ or "Integer" in srctype.__name__:

        op_num = select_number(src_stat, src_data, type="op")
        op_select = random.choice(OP)
        comp_num = select_number(src_stat, src_data, type="comp")

        candidate = [
            src_col.__eq__(comp_num),
            src_col.__le__(comp_num),
            src_col.__lt__(comp_num),
            src_col.__ne__(comp_num),
            src_col.__gt__(comp_num),
            src_col.__ge__(comp_num),
            src_col.op(op_select)(op_num).__eq__(comp_num),
            src_col.op(op_select)(op_num).__le__(comp_num),
            src_col.op(op_select)(op_num).__lt__(comp_num),
            src_col.op(op_select)(op_num).__ne__(comp_num),
            src_col.op(op_select)(op_num).__gt__(comp_num),
            src_col.op(op_select)(op_num).__ge__(comp_num)]

        return random.choice(candidate)

    elif "Date" in srctype.__name__:
        random_date = random.choice(src_data)
        op_select = random.choice(OP)
        op_num = select_number(src_stat, src_data, type="op")

        candidate = [
            src_col.__eq__(random_date),
            src_col.__le__(random_date),
            src_col.__lt__(random_date),
            src_col.__ne__(random_date),
            src_col.__gt__(random_date),
            src_col.__ge__(random_date),
            src_col.op(op_select)(op_num).__eq__(random_date),
            src_col.op(op_select)(op_num).__le__(random_date),
            src_col.op(op_select)(op_num).__lt__(random_date),
            src_col.op(op_select)(op_num).__ne__(random_date),
            src_col.op(op_select)(op_num).__gt__(random_date),
            src_col.op(op_select)(op_num).__ge__(random_date)]

        return random.choice(candidate)


def cond_generator(src_col, dst_col, src_stat, dst_stat, src_data):
    """
    input:  columns (left and/or right operand),
    return: column with condition (operator)
    """

    # 1) column and value
    srctype = str(src_col.type)

    if "CHAR" in srctype:
        # startword = select_word(src_data, type="start")
        # endword = select_word(src_data, type="end")
        # ilikeword = select_word(src_data, type="ilike")
        randomword = select_word(src_data, type="random")
        # containword = select_word(src_data, type="contain")

        op_num = select_number(src_stat, src_data, type="op")
        op_select = random.choice(OP)

        candidate = [
            src_col.__eq__(randomword),
            src_col.__le__(randomword),
            src_col.__lt__(randomword),
            src_col.__ne__(randomword),
            src_col.__gt__(randomword),
            src_col.__ge__(randomword),
            src_col.op(op_select)(op_num).__eq__(randomword),
            src_col.op(op_select)(op_num).__le__(randomword),
            src_col.op(op_select)(op_num).__lt__(randomword),
            src_col.op(op_select)(op_num).__ne__(randomword),
            src_col.op(op_select)(op_num).__gt__(randomword),
            src_col.op(op_select)(op_num).__ge__(randomword)]

        return random.choice(candidate)

    elif "FLOAT" in srctype or "INT" in srctype:

        op_num = select_number(src_stat, src_data, type="op")
        op_select = random.choice(OP)
        comp_num = select_number(src_stat, src_data, type="comp")

        candidate = [
            src_col.__eq__(comp_num),
            src_col.__le__(comp_num),
            src_col.__lt__(comp_num),
            src_col.__ne__(comp_num),
            src_col.__gt__(comp_num),
            src_col.__ge__(comp_num),
            src_col.op(op_select)(op_num).__eq__(comp_num),
            src_col.op(op_select)(op_num).__le__(comp_num),
            src_col.op(op_select)(op_num).__lt__(comp_num),
            src_col.op(op_select)(op_num).__ne__(comp_num),
            src_col.op(op_select)(op_num).__gt__(comp_num),
            src_col.op(op_select)(op_num).__ge__(comp_num)]

        return random.choice(candidate)

    elif "DATE" in srctype or "TIME" in srctype:
        random_date = random.choice(src_data)
        op_select = random.choice(OP)
        op_num = select_number(src_stat, src_data, type="op")

        candidate = [
            src_col.__eq__(random_date),
            src_col.__le__(random_date),
            src_col.__lt__(random_date),
            src_col.__ne__(random_date),
            src_col.__gt__(random_date),
            src_col.__ge__(random_date),
            src_col.op(op_select)(op_num).__eq__(random_date),
            src_col.op(op_select)(op_num).__le__(random_date),
            src_col.op(op_select)(op_num).__lt__(random_date),
            src_col.op(op_select)(op_num).__ne__(random_date),
            src_col.op(op_select)(op_num).__gt__(random_date),
            src_col.op(op_select)(op_num).__ge__(random_date)]

        return random.choice(candidate)


def func_generator(src_col, dst_col, src_stat, dst_stat, src_data):
    """
    input:  columns (left and/or right operand),
    return: column with function
    """

    # 1) column and value
    srctype = str(src_col.type)

    if "CHAR" in srctype:
        startword = select_word(src_data, type="start")
        endword = select_word(src_data, type="end")
        ilikeword = select_word(src_data, type="ilike")
        randomword = select_word(src_data, type="random")
        containword = select_word(src_data, type="contain")

        candidate = [
            src_col.notlike(randomword),
            src_col.like(randomword),
            src_col.notlike(randomword),
            src_col.ilike(ilikeword),
            src_col.notilike(ilikeword),
            src_col.contains(containword),
            src_col.concat(-1.33),
            src_col.startswith(startword),
            src_col.endswith(endword),
            src_col.collate('NOCASE'),
            src_col.isnot(randomword),
            src_col.is_(randomword),
            src_col.notin_(randomword),
            src_col.in_(randomword),
            src_col.desc(),
            src_col.asc()]

        return random.choice(candidate)

    elif "FLOAT" in srctype or "INT" in srctype or "SERIAL" in srctype:

        op_num = select_number(src_stat, src_data, type="op")
        op_select = random.choice(OP)
        # comp_num = select_number(src_stat, src_data, type="comp")
        selected_func = random.choice(FUNC_INDEX)

        candidate = [
            selected_func(src_col).__eq__(src_col),
            selected_func(src_col).__le__(src_col),
            selected_func(src_col).__lt__(src_col),
            selected_func(src_col).__ne__(src_col),
            selected_func(src_col).__gt__(src_col),
            selected_func(src_col).__ge__(src_col),
            selected_func(src_col).op(op_select)(op_num).__eq__(src_col),
            selected_func(src_col).op(op_select)(op_num).__le__(src_col),
            selected_func(src_col).op(op_select)(op_num).__lt__(src_col),
            selected_func(src_col).op(op_select)(op_num).__ne__(src_col),
            selected_func(src_col).op(op_select)(op_num).__gt__(src_col),
            selected_func(src_col).op(op_select)(op_num).__ge__(src_col)]

        return random.choice(candidate)

    elif "DATE" in srctype or "TIME" in srctype:
        random_date = random.choice(src_data)
        op_select = random.choice(OP)
        op_num = select_number(src_stat, src_data, type="op")
        selected_func = random.choice(FUNC_DATETIME_LIST)

        candidate = [
            selected_func(src_col).__eq__(random_date),
            selected_func(src_col).__le__(random_date),
            selected_func(src_col).__lt__(random_date),
            selected_func(src_col).__ne__(random_date),
            selected_func(src_col).__gt__(random_date),
            selected_func(src_col).__ge__(random_date),
            selected_func(src_col).op(op_select)(op_num).__eq__(random_date),
            selected_func(src_col).op(op_select)(op_num).__le__(random_date),
            selected_func(src_col).op(op_select)(op_num).__lt__(random_date),
            selected_func(src_col).op(op_select)(op_num).__ne__(random_date),
            selected_func(src_col).op(op_select)(op_num).__gt__(random_date),
            selected_func(src_col).op(op_select)(op_num).__ge__(random_date)]

        return random.choice(candidate)


def where_func_generator(src_col, dst_col, src_stat, dst_stat, src_data):
    """
    input:  columns (left and/or right operand),
    return: column with function
    """

    # 1) column and value
    srctype = str(src_col.type)

    if "CHAR" in srctype:
        startword = select_word(src_data, type="start")
        endword = select_word(src_data, type="end")
        ilikeword = select_word(src_data, type="ilike")
        randomword = select_word(src_data, type="random")
        containword = select_word(src_data, type="contain")

        candidate = [
            src_col.notlike(randomword),
            src_col.like(randomword),
            src_col.notlike(randomword),
            src_col.ilike(ilikeword),
            src_col.notilike(ilikeword),
            src_col.contains(containword),
            src_col.concat(-1.33),
            src_col.startswith(startword),
            src_col.endswith(endword),
            src_col.collate('NOCASE'),
            src_col.isnot(randomword),
            src_col.is_(randomword),
            src_col.notin_(randomword),
            src_col.in_(randomword),
            src_col.desc(),
            src_col.asc()]

        return random.choice(candidate)

    elif "FLOAT" in srctype or "INT" in srctype or "SERIAL" in srctype:

        op_num = select_number(src_stat, src_data, type="op")
        op_select = random.choice(OP)
        comp_num = select_number(src_stat, src_data, type="comp")
        selected_func = random.choice(FUNC_LIST)

        candidate = [
            selected_func(src_col).__eq__(comp_num),
            selected_func(src_col).__le__(comp_num),
            selected_func(src_col).__lt__(comp_num),
            selected_func(src_col).__ne__(comp_num),
            selected_func(src_col).__gt__(comp_num),
            selected_func(src_col).__ge__(comp_num),
            selected_func(src_col).op(op_select)(op_num).__eq__(comp_num),
            selected_func(src_col).op(op_select)(op_num).__le__(comp_num),
            selected_func(src_col).op(op_select)(op_num).__lt__(comp_num),
            selected_func(src_col).op(op_select)(op_num).__ne__(comp_num),
            selected_func(src_col).op(op_select)(op_num).__gt__(comp_num),
            selected_func(src_col).op(op_select)(op_num).__ge__(comp_num)]

        return random.choice(candidate)

    elif "DATE" in srctype or "TIME" in srctype:
        random_date = random.choice(src_data)
        op_select = random.choice(OP)
        op_num = select_number(src_stat, src_data, type="op")
        selected_func = random.choice(FUNC_DATETIME_LIST)

        candidate = [
            selected_func(src_col).__eq__(random_date),
            selected_func(src_col).__le__(random_date),
            selected_func(src_col).__lt__(random_date),
            selected_func(src_col).__ne__(random_date),
            selected_func(src_col).__gt__(random_date),
            selected_func(src_col).__ge__(random_date),
            selected_func(src_col).op(op_select)(op_num).__eq__(random_date),
            selected_func(src_col).op(op_select)(op_num).__le__(random_date),
            selected_func(src_col).op(op_select)(op_num).__lt__(random_date),
            selected_func(src_col).op(op_select)(op_num).__ne__(random_date),
            selected_func(src_col).op(op_select)(op_num).__gt__(random_date),
            selected_func(src_col).op(op_select)(op_num).__ge__(random_date)]

        return random.choice(candidate)


def where_generator_sample(target, purpose, update_dict):

    column_list = ret_columnlist(target.c)
    selected_name = random.choice(column_list).split(".")[1]
    selected_col = getattr(target.c, selected_name)
    selected_col = getattr(target.c, "length")
    string_col = getattr(target.c, "name")

    if purpose == "update":
        target = target.update()
    if purpose == "delete":
        target = target.delete()

    print ("COL_TYPE:", str(selected_col.type))

    if "CHAR" in str(selected_col.type):
        cond = selected_col.notlike("a")
        cond = cond.__and__(selected_col.like("a"))
        cond = cond.__and__(selected_col.ilike("a"))  # lower like
        cond = cond.__and__(selected_col.notilike("a"))  # lower notlike
        cond = cond.__and__(selected_col.contains("a"))
        cond = cond.__and__(selected_col.concat(-1.33))
        cond = cond.__and__(selected_col.startswith("a"))
        cond = cond.__and__(selected_col.endswith("a"))
        cond = cond.__and__(selected_col.collate('NOCASE'))
        cond = cond.__and__(selected_col.isnot('a'))
        cond = cond.__and__(selected_col.is_('a'))
        cond = cond.__and__(selected_col.notin_('abb'))
        cond = cond.__and__(selected_col.in_('abb'))

        # case something to string

        # finalize
        target = target.where(cond)

    elif "INT" in str(selected_col.type):

        # compare with column
        # op: + - * / % ++ &  >> <<  | ||
        # comp: __eq__, __le__, __lt__, __ne__, __ge__, __gt__
        cond2 = selected_col.op("+")(5)
        cond = selected_col.op("||")(5).__ge__(cond2)

        # compare with value
        cond = cond.__and__(selected_col.op("=")(5).__ge__(5))
        # option to choose either compare with  value or column

        # __neg__: make negative (should be option)
        cond = cond.__and__(selected_col.op("+")(5).__neg__().__ge__(5))

        # cast string to number
        # cast string_col to number
        cond = cond.__and__(selected_col.op("-")(5).__le__(
            cast(string_col, Float)))
        # idx2 = Index('idx2', users.c.id < cast(users.c.id, Float))

        # finalize
        target = target.where(cond)

    # retobj = target.where(selected_col.op("*")(5) == 0).values(update_dict)
    # print (type((selected_col.op("*")(5) == 0)))
    # target = target.where( and_( or_(selected_col.op("*")(5).__eq__(0), \
    #  selected_col.op("*")(5).__le__(3), selected_col.op("*")(5).__le__(4)), \
    #  selected_col.op("/")(5) == 0))

    if update_dict is not None:
        target = target.values(update_dict)
        retobj = target

    return retobj


""" types
VARCHAR(30)
BIGINT
INTEGER
REAL
FLOAT
BOOLEAN
NUMERIC(16, 8)
DATETIME
"""
