import random
from sqlfuzz.randoms import *
from sqlfuzz.wheregen import *
import sqlfuzz.conf as conf
import sys
import traceback
# This class is for AST node generation


class alc_tables(object):
    # helper class for generating statements
    def __init__(self, alc_tables):
        self.tables = alc_tables

    @staticmethod
    def get_alc_table(self, idx):
        return self.tables(idx)


class aliased_relation(object):
    def __init__(self, name, table_idx):
        self.name = name
        self.table_idx = table_idx


class Scope(object):
    # to model column/table reference visibility
    # store a list of subquery named as "d_int"
    # when the size exceeds a given limit, randomly remove one
    table_ref_list = []
    # this is for scalar subquery construction
    table_ref_stmt_list = []

    def __init__(self,
                 columnlist=None,
                 tablelist=None,
                 parent=None,
                 table_spec=None):
        # available for column and table ref
        if (columnlist is not None):
            self.column_list = columnlist
        else:
            self.column_list = []
        if (tablelist is not None):
            self.table_list = tablelist
        else:
            self.table_list = []
        # self.spec = table_spec
        if parent is not None:
            # self.spec = parent.spec
            self.column_list = parent.column_list
            self.table_list = parent.table_list
            self.stmt_seq = parent.stmt_seq
        self.alc_tables = None
        # Counters for prefixed stmt-unique identifiers
        # shared_ptr<map<string,unsigned int> >
        # "ref_" to an index
        self.stmt_seq = {}
        print("running constructor for scope")

    def add_alc(self, alc_tables):
        self.alc_tables = alc_tables


class Prod(object):
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        self.pprod = parent
        self.name = name
        # record table's column and its type
        self.spec = spec
        # record tables' simple stat
        self.spec_stat = spec_stat
        # model column/table reference visibility
        self.scope = scope


class Value_Expr(Prod):
    # code for generating expr just depends on Prod
    # TODO: probability table
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        super().__init__(name, spec, spec_stat, scope, parent)
        # possible output
        # 1. a random column
        # 2. a function call

    def gen_random_column(self, table_idx=None, column_number=None):
        if table_idx is None:
            random_table_idx = random.choice(self.scope.table_list)
        else:
            random_table_idx = table_idx
        column_names = self.spec_stat[random_table_idx].column_name
        num_cols = len(column_names)
        if column_number is None:
            chosen_columns = random.choices(
                column_names, k=randoms.random_int_range(num_cols))
        else:
            chosen_columns = random.choices(column_names, k=column_number)
        print("picking random columns from",
              self.spec_stat[random_table_idx].tablename, chosen_columns)
        #todo: should be dictionary
        return chosen_columns, random_table_idx


class Table_Ref(Prod):
    # code for generating reference to a general table, just depends on Prod
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        super().__init__(name, spec, spec_stat, scope, parent)
        # TODO: for index table in refs, might need it for stmt_uid
        # self.key = 0
        self.refs = []
        print("running constructor for table reference")


class From_Clause(Prod):
    # static variable for the class, store a list of subquery from previous runs
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        print("running constructor for from clause")
        super().__init__(name, spec, spec_stat, scope, parent)
        # store a table_ref pointers
    def get_random_from_clause(self, force_simple=False):
        # branch choice either simple table, existing subquery, or new joined stmt
        if (force_simple is True):
            # simple base table
            return random.choice(self.scope.alc_tables), None, None
        if (random_int_range(1000) < conf.PROB_TABLE["simple"]):
            if len(Scope.table_ref_list) == 0:
                # simple means no join operator
                return random.choice(self.scope.alc_tables), None, None
            print("select a simple table from alc_tables or subquery")
            return random.choice(Scope.table_ref_list +
                                 self.scope.alc_tables), None, None
        else:
            # create a joined_table by joinning two table_ref
            # todo: also choice from table_ref_list
            return self.get_joined_table()

    def get_joined_table(self):
        # step 1. get two table_ref
        # step 2. perform join operation on two table_ref
        table_a = random.choice(Scope.table_ref_list + self.scope.alc_tables)
        table_b = random.choice(Scope.table_ref_list + self.scope.alc_tables)
        while (table_a == table_b):
            table_b = random.choice(self.scope.alc_tables)
        print("create a joined_table by joinning two table_ref")
        random_column = random.choice(get_selectable_column(table_a))
        join_condition = None
        join_type = None
        j = None
        for c in table_b.columns:
            # print(c.type)
            # if (c.type is (random_column.type)):
            if (isinstance(c.type, type(random_column.type))):
                srctype = str(c.type)
                if ("FLOAT" in srctype or "INT" in srctype) is False:
                    continue
                # generate join type
                print("find match type")
                if (random_int_range(1000) < conf.PROB_TABLE["true"]):
                    print("true")
                    join_condition = true()
                else:
                    # on a condition
                    print("condition")
                    join_condition = join_cond_generator(c, random_column)
                # join_condition = join_cond_generator(c, random_column)
                # print("join condition is", literalquery(join_condition))
                if (random_int_range(1000) < conf.PROB_TABLE["inner"]):
                    # inner join will override previous join condition generation
                    join_condition = join_cond_generator(c, random_column)
                    join_type = "inner"
                    j = table_a.join(table_b, join_condition)
                    # print("succes inner join")

                elif (random_int_range(1000) < conf.PROB_TABLE["full"]):
                    # cross join
                    # print("cross")
                    join_type = "cross"
                    try:
                        j = table_a.outerjoin(table_b,
                                              join_condition,
                                              full=True)
                    except:
                        j = table_a.outerjoin(table_b, true(), full=True)
                    # print(literalquery(j))
                else:
                    # left join
                    # print("left", join_condition)
                    join_type = "left"
                    try:
                        j = table_a.join(table_b, join_condition, isouter=True)
                    except:
                        j = table_a.join(table_b, true(), isouter=True)
                break
        if (j is not None):
            print("success join")
            return table_a, table_b, j
        else:
            return table_a, None, None


class Select_List(Prod):
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        print("running constructor for select list")
        super().__init__(name, spec, spec_stat, scope, parent)
        self.value_exprs = []
        # columns is for subquery constrction, renaming purpose
        self.columns = 0
        # derived_table is for gen_select_statement and subquery construction
        self.derived_table = {}
        # self.selectable_columns = []

    def gen_select_expr(self, from_clause, number_columns=None):
        # output: expression, table_idx, chose_column names(str)
        selectable_columns = get_selectable_column(from_clause)
        selectable_columns_length = len(selectable_columns)
        if (number_columns is None):
            number_columns = random_int_range(selectable_columns_length)
        chosen_columns_obj = random.sample(selectable_columns, number_columns)
        # chosen_columns, table_idx = (expression.gen_random_column(column_number=number_columns))
        out = chosen_columns_obj
        chosen_columns = [str(i).split(".")[-1] for i in out]
        print("chosen columns is", chosen_columns)

        for i in range(len(out)):
            if (random_int_range(1000) < conf.PROB_TABLE["literal_column"]):
                print("generate literal_column")
                new_type = (out[i].type)
                new_type_str = str(out[i].type)
                literal_column_obj = None
                if "CHAR" in new_type_str:
                    literal_string = "'%s'" % conf.LITERAL_COLUMN_STR
                    literal_column_obj = literal_column(literal_string,
                                                        type_=String)
                elif "FLOAT" in new_type_str or "INT" in new_type_str:
                    literal_column_obj = literal_column(
                        conf.LITERAL_COLUMN_INT, type_=Integer)
                if literal_column_obj is not None:
                    out[i] = type_coerce(literal_column_obj, new_type).label(
                        name="c_" + str(random.randint(1, 5000)))

        # candidate.append(case_clause)
        #functions to increase the variety of selectable objects
        for i in range(len(out)):
            if (random_int_range(1000) < conf.PROB_TABLE["func_expr"]):
                if (random_int_range(1000) < conf.PROB_TABLE["nested"]):
                    # if success, break
                    # first generate distinct
                    new_type = out[i].type
                    out[i] = type_coerce((func.distinct(out[i])),new_type).label(name="c_" +str(random.randint(1, 5000)))
                # second generate function
                print("gen func")
                selectable_func_list = get_compatible_function(out[i])
                selected_func = random.choice(selectable_func_list)
                print(type(selected_func))
                new_type = out[i].type
                # only count function would change the type of the column
                if selected_func == func.count:
                    new_type = Float
                if (random_int_range(1000) < conf.PROB_TABLE["window"]):
                    if (random_int_range(1000) > 750):
                        out[i] = type_coerce(
                            (selected_func(out[i])).over(
                                partition_by=random.sample(
                                    selectable_columns, 1),
                                order_by=random.sample(selectable_columns, 1)),
                            new_type).label(name="c_" +
                                            str(random.randint(1, 5000)))
                    elif (random_int_range(1000) > 500):
                        out[i] = type_coerce((selected_func(out[i])).over(
                            partition_by=random.sample(selectable_columns, 1)),
                                             new_type).label(
                                                 name="c_" +
                                                 str(random.randint(1, 5000)))
                    elif (random_int_range(1000) > 250):
                        out[i] = type_coerce(
                            (selected_func(out[i])).over(
                                order_by=random.sample(selectable_columns, 1)),
                            new_type).label(name="c_" +
                                            str(random.randint(1, 5000)))
                    else:
                        out[i] = type_coerce(
                            (selected_func(out[i])).over(),
                            new_type).label(name="c_" +
                                            str(random.randint(1, 5000)))
                else:
                    out[i] = type_coerce(
                        (selected_func(out[i])),
                        new_type).label(name="c_" +
                                        str(random.randint(1, 5000)))

                # # window
                # out[i] = out[i]
        self.value_exprs = out
        # get all selectable column names from the given table
        # all_column_names = self.spec_stat[table_idx].column_name
        # selectable_column = []
        # for item in all_column_names:
        #     selected_col = getattr(self.scope.alc_tables[table_idx].c, item)
        #     selectable_column.append(selected_col)
        # self.selectable_columns = selectable_column
        # print ((self.value_exprs), table_idx, chosen_columns)
        return (self.value_exprs), None, selectable_columns
        # 2. update derived table for subquery construction purpose


class Query_Spec(Prod):
    # top class for generating select statement
    def __init__(self, name, spec, spec_stat, scope, parent=None):
        super().__init__(name, spec, spec_stat, scope, parent)
        self.from_clause = []
        self.select_list = []
        self.limit_clause = None
        self.offset_clause = None
        self.scope = scope
        self.entity_list = []
        print("running constructor for query_spec")

    def get_table_idx_from_column_name(self, column_name):
        # input: convoluted column name resulting from alias rename
        # output: table_idx and correspond simple columname
        suffix_column_name = column_name.split(".")[-1]
        print("column_name is", suffix_column_name)
        for i in range(len(self.spec_stat)):
            t_spec = self.spec_stat[i]
            for c in t_spec.column_name:
                if c in suffix_column_name:
                    print("table idx found", i)
                    return i, c
        return None, None

    def gen_select_statement(self,
                             select_column_number=None,
                             force_simple_from=False):
        # parameter needed: prod, scope

        # 1. ########## generate from_clause ##########
        #     get a random table
        base_table = False
        print("running simple constructor for select from")
        from_ins = From_Clause(self.name, self.spec, self.spec_stat,
                               self.scope)
        print("table_ref_list", Scope.table_ref_list)
        from_clause1, from_clause2, joined_from = from_ins.get_random_from_clause(
            force_simple=force_simple_from)
        print("from_clause is", from_clause1, from_clause2, joined_from)
        if ("Table" in str(type(from_clause1))):
            base_table = True
        print(type(from_clause1), type(from_clause2))
        # ########## should decide where to select from by this point ##########
        # 2. generate select_expr
        select_list = Select_List(self.name, self.spec, self.spec_stat,
                                  self.scope)
        # TODO: the function call gen_select_expr should be like a loop that update derived table
        if (joined_from is not None):
            select_list_expr, _, selectable_columns_obj = select_list.gen_select_expr(
                joined_from, number_columns=select_column_number)
        else:
            select_list_expr, _, selectable_columns_obj = select_list.gen_select_expr(
                from_clause1, number_columns=select_column_number)

        # return None, None
        # for join cases, only one joined item would affect the where clause
        where_clause = self.gen_where_clause(select_list_expr, None,
                                             selectable_columns_obj)
        # print(select_list_expr, where_clause)
        # print(where_clause)

        selectable_columns = []
        if joined_from is not None:
            selectable_columns = get_selectable_column(
                from_clause1) + get_selectable_column(from_clause2)
        else:
            selectable_columns = get_selectable_column(from_clause1)
        return (select_list_expr
                ), where_clause, None, selectable_columns, joined_from, base_table

    def gen_where_clause(self, select_list_expr, table_idx,
                         selectable_columns_obj):
        # generate where clause according to the slqalchemy column
        # 1) select a sql alchemy column

        CONJ = ["and", "or"]

        num_where = min(3, random_int_range(len(selectable_columns_obj)))
        where_clause_list = []
        print("enter gen_where_clause")
        for i in range(num_where):
            random_column_object = random.choice(selectable_columns_obj)
            # get the column object
            try:
                # handle joined table where column names do not always belong to the same table
                # column = getattr(self.scope.alc_tables[table_idx].c,
                #              random_column_name)
                columnname = str(random_column_object).split(".")[-1]
                print("columnname is", columnname, random_column_object.type)
                table_idx, columnname = self.get_table_idx_from_column_name(
                    columnname)
                if table_idx is None:
                    # this is not an original column
                    print("fail found table_idx")
                    column_where = where_generator(random_column_object, None,
                                                   None, None, None)
                else:
                    print("found table idx for content-aware where generator")
                    print(self.spec[table_idx].table_name)
                    column_stat = self.spec_stat[table_idx].ret_stat(
                        columnname)
                    column_data = self.spec_stat[table_idx].ret_string(
                        columnname)
                    column_where = where_generator(random_column_object, None,
                                                   column_stat, None,
                                                   column_data)
                if column_where is not None:
                    where_clause_list.append(column_where)
            except Exception as inst:
                print("fail in gen where", inst, traceback.print_exc())
                continue
        # try add one more filter from subquery
        if random_int_range(1000) < conf.PROB_TABLE["subquery"] and len(
                Scope.table_ref_stmt_list) > 0:
            random_idx_list = list(range(len(Scope.table_ref_stmt_list)))
            random.shuffle(random_idx_list)

            # choose from an existing stmt
            for idx in random_idx_list:
                s = Scope.table_ref_stmt_list[idx]
                # subquery has to return only one column
                if len(s.columns) == 1:
                    srctype = str(get_selectable_column(s)[0].type)
                    column_stat = None
                    column_data = None
                    print("orig subquery type", srctype)
                    # try to find a compatible column from the outer query
                    comp_column = get_compatible_column(selectable_columns_obj, srctype)
                    if comp_column is not None:
                        print("comp column is", type(select([get_selectable_column(s)[0]])))
                        # print(type(s_))
                        # print(literalquery(s_) + ";")
                        print(comp_column, type(comp_column))
                        criteria = None
                        if random_int_range(1000) < 500:
                            # exist subquery
                            # exists_criteria = exists(select([get_selectable_column(s)[0]]).where(comp_column == get_selectable_column(s)[0]))
                            # exists_criteria = select([get_selectable_column(s)[0]]).where(get_selectable_column(s)[0] ==5 )
                            exists_criteria = exists(s)
                            criteria = exists_criteria
                        else:
                            # in subquery
                            in_criteria = comp_column.in_(s)
                            criteria = in_criteria
                        if criteria is not None:
                            if random_int_range(1000) < 500:
                                where_clause_list.append(criteria)
                            else:
                                where_clause_list.append(~criteria)
                            print("success subquery")
                            break
        # try add one more filter from scalar subquery
        if random_int_range(1000) < conf.PROB_TABLE["scalar"] and len(
                Scope.table_ref_stmt_list) > 0:
            # choose from an existing stmt
            random_idx_list = list(range(len(Scope.table_ref_stmt_list)))
            random.shuffle(random_idx_list)

            # choose from an existing stmt
            for idx in random_idx_list:
                s = Scope.table_ref_stmt_list[idx]
                if len(s.columns) == 1:
                    srctype = str(get_selectable_column(s)[0].type)
                    column_stat = None
                    column_data = None
                    print("orig scalar subquery type", srctype)
                    if "CHAR" in srctype:
                        column_data = [conf.SCALAR_STR]
                    elif "FLOAT" in srctype or "INT" in srctype or "NUMERIC" in srctype:
                        column_data = [int(conf.SCALAR_INT)]
                        column_stat = [int(conf.SCALAR_INT)]
                    else:
                        continue
                    scalar_stmt = s.limit(1).as_scalar()
                    column_where = where_generator(scalar_stmt, None,
                                                   column_stat, None,
                                                   column_data)
                    if column_where is not None:
                        where_clause_list.append(column_where)
                        print("success scalar subquery")
                        # at most one scalar subquery
                        break

        # begin merging the where clause
        parenthesis = False
        while (len(where_clause_list) > 1):
            where1 = where_clause_list[0]
            if where1 is None:
                where_clause_list.remove(where1)
                continue
            where2 = where_clause_list[1]
            if where2 is None:
                where_clause_list.remove(where2)
                continue
            combined_where = None
            if parenthesis is False:
                combined_where = combine_condition(where1, where2,
                                                   random.choice(CONJ))
                parenthesis = True
            else:
                combined_where = combine_parenthesis(where1, where2,
                                                     random.choice(CONJ))
            where_clause_list.remove(where1)
            where_clause_list.remove(where2)
            where_clause_list.insert(0, combined_where)

        if len(where_clause_list) > 0:
            return where_clause_list[0]
        else:
            return false()

        # 2. generate select list
        # 3. generate distinct
        # 4. generate limit
