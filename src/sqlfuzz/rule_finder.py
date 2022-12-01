#!/usr/bin/env python3
import os
import matplotlib.pyplot as plt
import numpy as np
import argparse
import subprocess
import argparse
import json
import concurrent.futures
import subprocess
import os
import json
import sys
import shutil
from threading import Timer
import time
import random
from collections import Counter
import psycopg2
import re
import time
from shutil import copyfile
from sqlfuzz.rule import *
from collections import Counter, defaultdict
import pandas as pd
import numpy as np
import seaborn as sns
import pylab
import matplotlib as mpl
from matplotlib.backends.backend_pdf import PdfPages

from datetime import datetime
from matplotlib.font_manager import FontProperties
LABEL_FONT_SIZE = 14

LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
OPT_GRAPH_HEIGHT = 300
OPT_GRAPH_WIDTH = 400
# from sets import Set
FUNC_LIST = ["sum", "max", "min", "abs", "avg", "count", "variance"]
SCALAR_STR = "scalar string"
SCALAR_FLOAT = "4666"
SET_OPERATION = ["UNION", "UNION_ALL", "INTERSECT", "INTERSECT_ALL", "EXCEPT_", "EXCEPT_ALL"]
LITERAL_COLUMN_STR = "literal col string"
LITERAL_COLUMN_INT = "2333"
NON_SCALAR_SUBQUERY = ["EXISTS", "NOT IN", "IN"]
# 3 kinds of entites that need careful update: both increasing and decreasing
competing_entities_table_ref = ["SIMPLE"]
competing_entities_join_cond = ["TRUE"]
competing_entities_join_type = ["INNER", "LEFT", "FULL"]
def saveGraph(fig, outname, width, height):
    size = fig.get_size_inches()
    dpi = fig.get_dpi()

    new_size = (width / float(dpi), height / float(dpi))
    fig.set_size_inches(new_size)
    new_size = fig.get_size_inches()
    new_dpi = fig.get_dpi()

    pp = PdfPages(outname)
    fig.savefig(pp, format='pdf', bbox_inches='tight')
    pp.close()
    return
def co_occurrence(label_data):
    # print(sentences)
    cooccurrence_matrix = np.dot(label_data.transpose(),label_data)
    return cooccurrence_matrix

def analyze_single_query(string, prob_table):
    # used by the validator
    # the string is output from the SA, where the keyword is uppercase
    # return a list of entities that occur in the interesting original query
    # has to parse meta info
    extract_entities = []

    # extract keyword in query string
    for k in prob_table:
        if k.upper() in string and k.upper() != "TRUE":
            extract_entities.append(k)

    # extract by signature for other entities
    if "JOIN" not in string:
    # simple table query is still interesting
        extract_entities.append("simple")
    if "JOIN" in string:
        join_count = string.count("JOIN")
        true_count = string.count("TRUE")
        if (join_count > true_count and true_count == 0):
            # only boolean expression appear in join condition
            extract_entities.append("bool_expr_join")
        elif (join_count == true_count):
            # all join condition is true
            extract_entities.append("true")

    if LITERAL_COLUMN_STR in string or LITERAL_COLUMN_INT in string:
        extract_entities.append("literal_column")

    if SCALAR_STR in string or SCALAR_FLOAT in string:
        extract_entities.append("scalar")

    for i in SET_OPERATION:
        if i in string:
            extract_entities.append("set")
            break
    for func in FUNC_LIST:
        if func.upper() in string:
            extract_entities.append("func_expr")
    # window
    if "OVER" in string:
        extract_entities.append("window")
    # subquery
    # not update bc it generates too many complicate queries
    # for sig in NON_SCALAR_SUBQUERY:
    #     if sig in string:
    #         extract_entities.append("subquery")
    # year and month
    if "year" in string:
        extract_entities.append("extractyear")
    if "month" in string:
        extract_entities.append("extractmonth")
    # function nested distinct
    if "distinct" in string:
        for func in FUNC_LIST:
            if func + "(" + "distinct" in string:
                extract_entities.append("nested")
                break
    return extract_entities

def parse_original_query(filename):
    # used by the validator
    with open(filename) as f:
      s = " ".join([x.strip() for x in f])
    queries = s.split(";")
    original_query_idx = 0
    for i in range(len(queries)-1):
        if ("-- original" in queries[i]):
            original_query_idx = i-1
    return queries[original_query_idx]

def get_fired_rules_base_query(queries):
    fired_rule_list_query = []
    for i in range(len(queries)-1):
        if ("--" in queries[i]):
            fired_rule_list_ = (queries[i].split("-- ")[1].split(","))
            fired_rule_list = [x.strip() for x in fired_rule_list_]
            fired_rule_list = (fired_rule_list[:-1])
            # print(fired_rule_list)
            for r in fired_rule_list:
                if r not in fired_rule_list_query:
                    fired_rule_list_query.append(r)
    return fired_rule_list_query

def find_fired_rules_query_file(filename):
    with open(filename) as f:
      s = " ".join([x.strip() for x in f])

    queries = s.split(";")
    return get_fired_rules_base_query(queries)

def find_base_query_calcite(filename):
    # todo
    with open(filename) as f:
      s = " ".join([x.strip() for x in f])

    queries = s.split(";")
    # the first query is base, the latter is mutated
    return queries[0].lower()
def find_query_operator(query, tirggeredpattern):
    entities_dict = json.load(open(os.path.join(CONF_ROOT, "entities.json")))
    function_name = entities_dict["function_name"]
    keyword = entities_dict["keyword"]
    datatype = entities_dict["type"]
    joinoperator = entities_dict["join"]
    triggered_operator= set()
    # categorize all func into one, does not count data type
    for fn in function_name:
        if fn in query:
            triggered_operator.add("func")
            if fn not in tirggeredpattern["function_name"]:
                tirggeredpattern["function_name"].append(fn)
    for kw in keyword:
        if (kw != "in"):
            if kw in query:
                triggered_operator.add(kw)
                if kw not in tirggeredpattern["keyword"]:
                    tirggeredpattern["keyword"].append(kw)
        else:
            if kw in query and "join" not in query:
                triggered_operator.add(kw)
                if kw not in tirggeredpattern["keyword"]:
                    tirggeredpattern["keyword"].append(kw)
    for dt in datatype:
        if dt in query:
            if dt not in tirggeredpattern["type"]:
                tirggeredpattern["type"].append(dt)
    for jo in joinoperator:
        if jo in query:
            triggered_operator.add(jo)
            if jo not in tirggeredpattern["join"]:
                tirggeredpattern["join"].append(jo)
    if ("fetch") in query:
        triggered_operator.add("limit")
        if "limit" not in tirggeredpattern["keyword"]:
            tirggeredpattern["keyword"].append("limit")
    return list(triggered_operator)

def process_single_query_file(filename, fired_rules_dict):
    # used by validator
    with open(filename) as f:
      s = " ".join([x.strip() for x in f])

    queries = s.split(";")
    fired_rule_list_query = get_fired_rules_base_query(queries)
    # # #------------------extract fired rule in this query---------------
    # for i in range(len(queries)-1):
    #     if ("--" in queries[i]):
    #         fired_rule_list_ = (queries[i].split("-- ")[1].split(","))
    #         fired_rule_list = [x.strip() for x in fired_rule_list_]
    #         fired_rule_list = (fired_rule_list[:-1])
    #         # print(fired_rule_list)
    #         for r in fired_rule_list:
    #             if r not in fired_rule_list_query:
    #                 fired_rule_list_query.append(r)
    for i in fired_rule_list_query:
        if i != "original":
            if i not in fired_rules_dict:
                r = Rule(i)
                r.update_firing_query(filename)
                fired_rules_dict[i] = r
                print("new rule triggered", i)
            else:
                fired_rules_dict[i].update_firing_query(filename)
                # rule has been fired before
    # print(fired_rule_list_query)
    return fired_rules_dict

def missed_fired_rule(filename):
    # the number of line starting with -- == the number of queries generated
    fired_combo={}
    rule_list = []
    #-----------------------histogram for chain of fired rules-----------
    # with open(("rule"),"r") as f:
    #     lines = f.readlines()
    #     for line in lines:
    #         parsed_line = line.split("-- ")[1].split(",")
    #         fired_rules = []
    #         for rule in parsed_line:
    #             if " ;" not in rule:
    #                 fired_rules.append(str(rule.lstrip()))
    #         big_string=""
    #         for i in sorted(fired_rules):
    #             big_string += i
    #             big_string += "+"
    #         print(big_string)
    #         # fired_set = set(fired_rules)
    #         if big_string not in fired_combo:
    #             fired_combo[big_string]=1
    #         else:
    #             fired_combo[big_string]+=1
    #             # if " ;" not in rule:
    #             #     fired_rule = (rule.split()[0])
    #             #     if fired_rule not in rule_list:
    #             #         rule_list.append(fired_rule)
    # print("# of fired chains", len(fired_combo))
    # for x, y in fired_combo.items():
    #     print(x, y)
    # width = 1.0     # gives histogram aspect to the bar diagram
    # fig = plt.figure(figsize=(10, 30))
    # plt.xticks(rotation='vertical')
    # plt.yticks(np.arange(min(fired_combo.values()), max(fired_combo.values())+1, 1.0))

    # plt.bar(fired_combo.keys(),fired_combo.values(), width, color='g')
    # plt.show()
    # fig.savefig('temp.png', dpi=fig.dpi)
    #-------------------------find unfired rule---------------
    print(filename)
    with open((filename),"r") as f:
        lines = f.readlines()
        for line in lines:
            parsed_line = line.split("-- ")[1].split(",")
            for rule in parsed_line:
                if " ;" not in rule:
                    fired_rule = (rule.split()[0])
                    if fired_rule not in rule_list:
                        rule_list.append(fired_rule)
    fired_rule_set=set(rule_list)
    print("fired rule count: ", len(fired_rule_set))
    fired_rule_set=set(rule_list)
    print(len(fired_rule_set))
    # print("Union :", len(fired_rule_set | all_rule_set))
    # print("Intersection :", (fired_rule_set & all_rule_set))
    print("Not fired rules count:", len(all_rule_set-fired_rule_set))
    # count = 0
    print("rule that should fire:")
    no_fired_rule = sorted(list(all_rule_set-fired_rule_set))
    missed_rule = []
    for i in no_fired_rule:
        if "MaterializedView" not in i and "Enumerable" not in i:
            # count+=1
            print(i)
            missed_rule.append(i)
    print("***********************************************************************")
    print("Weired fired rules", (fired_rule_set - all_rule_set))
    print("***********************************************************************")
    print("Fired rules:")
    list_fired_rule = sorted(list(fired_rule_set))

    for i in list_fired_rule:
        print(i)
    return missed_rule, list_fired_rule
def code_op_list(op_list, clause_name):
    l = [0] * len(clause_name)
    for o in op_list:
        idx = clause_name.index(o)
        l[idx] = 1
    return l
def main(args):
    global PROJECT_ROOT
    global CONF_ROOT

    PROJECT_ROOT = os.getenv('PROJECT_ROOT')
    CONF_ROOT = os.getenv('CONF_ROOT')

    experiment_folder_list = [i for i in (args.output).split(",")]
    print(experiment_folder_list)
    # experiment_folder = (os.path.join(PROJECT_ROOT, args.output))
    # global rule_list
    global all_rule_list
    global all_rule_set
    global fired_rules_dict
    total_executable_queries = 0
    rule_list = []
    all_rule_list=[]
    fired_rules_dict={}
    clause_name = []
    with open(os.path.join(CONF_ROOT, "entities.json"), "r") as f:
        op_dic = json.load(f)
        clause_name += op_dic["keyword"]
        clause_name += op_dic["join"]
        clause_name.append("func")
    clause_name = sorted(clause_name)
    idx_clause_name={}
    for c in range(len(clause_name)):
        idx_clause_name[c] = clause_name[c]
    print(idx_clause_name)
    print("total supported clauses", len(clause_name))
    with open(("all_rules"),"r") as f:
        lines = f.readlines()
        for line in lines:
            all_rule_list.append(line.split("\n")[0])
    all_rule_set=set(all_rule_list)
    print("total rules:", len(all_rule_set))
    importantpattern = {}
    importantpattern["function_name"] = []
    importantpattern["keyword"] = []
    importantpattern["type"] = []
    importantpattern["join"] = []
    important_bq_operator = []

    with open (args.bqfile) as f:
        lines = f.readlines()
        cnt = 0
        for l in lines:
            bq = find_base_query_calcite(l.rstrip())
            op_list = find_query_operator(bq, importantpattern)
            coded_op_list = code_op_list(op_list, clause_name)
            print(coded_op_list)
            important_bq_operator.append(coded_op_list)
        for k, v in (importantpattern).items():
            print(k, len(v), v)
    important_bq_operator_df = pd.DataFrame(important_bq_operator, columns = clause_name)
    co_ocurrence_matrix = co_occurrence(important_bq_operator_df)
    print(len(co_ocurrence_matrix))
    print(np.count_nonzero(co_ocurrence_matrix))
    important_idx = (np.nonzero(co_ocurrence_matrix))
    important_x = (important_idx[0])
    important_y = (important_idx[1])
    print("&************")
    print(co_ocurrence_matrix)
    matrix = []
    for exp in experiment_folder_list:
        experiment_folder = os.path.join(PROJECT_ROOT, str(exp))
        # read the executable_file
        bq_list = []
        bq_operator = []
        operator_frequency = {}
        with open(os.path.join(experiment_folder,"executable_queries.txt")) as f:
            lines = f.readlines()
            total_executable_queries += len(lines)
            # ------------------ fired rules analysis -----------
            # # iterate process executable query to update the rule dictionary
            # for l in lines:
            #     r_list = find_fired_rules_query_file(l.rstrip())
            #     if "AggregateExpandDistinctAggregatesRule" in r_list:
            #         print(l)
            # ------------------ base query analysis -----------
            triggeredpattern = {}
            triggeredpattern["function_name"] = []
            triggeredpattern["keyword"] = []
            triggeredpattern["type"] = []
            triggeredpattern["join"] = []
            cnt = 0
            for l in lines:
                bq = find_base_query_calcite(l.rstrip())
                bq_list.append(bq + ";")
                op_list = find_query_operator(bq, triggeredpattern)
                for o in op_list:
                    if o not in operator_frequency:
                        operator_frequency[o] = 1
                    else:
                        operator_frequency[o] += 1
                coded_op_list = code_op_list(op_list, clause_name)
                # print(coded_op_list)
                bq_operator.append(coded_op_list)
                cnt += 1
                if (cnt > 2000):
                    print("reach 2000")
                    break
            for k, v in (triggeredpattern).items():
                print(k, len(v), v)
            bq_operator_df = pd.DataFrame(bq_operator, columns = clause_name)
            co_ocurrence_matrix = co_occurrence(bq_operator_df)
            print(len(co_ocurrence_matrix))
            print(np.count_nonzero(co_ocurrence_matrix))

            # with open(os.path.join(args.output, "bqs.sql"), "w") as f:
            #     for e in bq_list:
            #         f.write("%s\n" % e)



            normalized_co_ocurrence_matrix=(co_ocurrence_matrix/cnt)
            # print(normalized_co_ocurrence_matrix.columns)
            matrix2save = pd.DataFrame(normalized_co_ocurrence_matrix)
            matrix2save.columns=clause_name
            matrix2save.rename(index=idx_clause_name, inplace=True)
            matrix.append(matrix2save)
            print("nonzero", np.count_nonzero(matrix2save))
# pd.DataFrame(np_array).to_csv("path/to/file.csv")

    f, (ax) = plt.subplots(1,2)
    f.set_size_inches(14, 10)
    an_array_list = []
    for m in range(len(matrix)):
        # set marker
        print(type(matrix[m]))
        an_array = np.empty((len(matrix[m]), len(matrix[m])), dtype = str)
        # mark the bug finding pattern with markers
        numpy_matrix = matrix[m].to_numpy()
        for i in range(len(important_x)):
            x = important_x[i]
            y = important_y[i]
            if (numpy_matrix[x][y] > 0):
                an_array[x][y] = "*"
            an_array_list.append(an_array)
            # an_array[7:20] = '^'
        if (m == 0):
            sns_plot = sns.heatmap(matrix[m], cmap="YlGnBu", vmax = 0.5, robust=True, ax=ax[m], cbar=False, mask=(matrix[m]==0))
        else:
            calcite_an_array = an_array_list[0]
            for ix,iy in np.ndindex(an_array.shape):
                # only mark combinations that are only discovered by AMOEBA
                if (an_array[ix, iy] == "*" and calcite_an_array[ix, iy] == "*"):
                    # print("reset")
                    an_array[ix, iy] = ""
                elif (an_array[ix, iy] == "*"):
                    print("don't need reset")
                else:
                    print(an_array[ix, iy], calcite_an_array[ix, iy])
            sns_plot = sns.heatmap(matrix[m], cmap="YlGnBu", vmax = 0.5, robust=True, ax=ax[m], cbar=False, mask=(matrix[m]==0), annot=an_array, fmt = '')


        if m == 0:
            ax[m].set_xlabel("CALCITE", fontproperties=LABEL_FP, fontweight='bold')
        else:
            ax[m].set_xlabel("AMOEBA", fontproperties=LABEL_FP, fontweight='bold')



    # ax1.set_ylabel("Speedup", fontproperties=LABEL_FP, fontweight='bold')
        # sns_plot1 = sns.heatmap(matrix2save, annot=False, cmap="YlGnBu", vmax = 0.3, robust=True, ax=ax[1], cbar=False)
    plt.subplots_adjust(left=0.1, right=0.99, top = 0.95, bottom=0.2)
    # plt.show()
    # matrix2save.to_csv(os.path.join(args.output,'matrix.csv'), index=True)
    # ax.remove()
    saveGraph(f,args.mapname + '.pdf',
              width=OPT_GRAPH_WIDTH * 2.2,
              height=OPT_GRAPH_HEIGHT * 1.8)
    #f.savefig(os.path.join(args.figoutput, args.mapname + '.pdf'))
    fig, ax = plt.subplots(figsize=(6, 1))
    fig.subplots_adjust(bottom=0.5)

    cmap = plt.get_cmap("YlGnBu")
    norm = mpl.colors.Normalize(vmin=0, vmax=0.5)

    cb1 = mpl.colorbar.ColorbarBase(ax, cmap=cmap,
                                    norm=norm,
                                    orientation='horizontal')
    cb1.set_label('Normalized Co-Occurence Frequency',fontproperties=LABEL_FP, fontweight='bold')
    fig.savefig(os.path.join(args.figoutput, "hotmapbar.pdf"))
    # plt.show()




    #     # --------------- begin analyze dictionary ----------------
    print("accumulative executable queries: ", total_executable_queries)
    # print("---------------analyze dictionary-------------------")
    # print("accumulative fired rules: ", len(fired_rules_dict))
    # fired_rules_list = []
    # rules_frequency = []
    # for k,v in fired_rules_dict.items():
    #     fired_rules_list.append(v.rule_name)
    #     rules_frequency.append(v.frequency)
    # fired_rules_list.sort()
    # # for r in fired_rules_list:
    # #     print(r)
    # rules_frequency.sort()
    # # print(rules_frequency)
    # # print(args.promote)
    # if (args.promote != 0):
    #     bottom_k_rules_frequency = rules_frequency[:int(args.promote*len(rules_frequency)/100)]
    #     print(bottom_k_rules_frequency)
    #     query_file_list=[]
    #     for k,v in fired_rules_dict.items():
    #         # print(k)
    #         if v.frequency in bottom_k_rules_frequency:
    #             # print(v.rule_name)
    #             # print(v.query_list)
    #             query_file_list += (v.query_list)


    #     # --------------- begin analyze query file and update prob_table  ----------------
    #     # load_prob_table
    #     prob_table = {}
    #     update_entities={}
    #     with open(args.prob_table) as f:
    #         print("Try read probability table", f.name)
    #         prob_table = json.load(f)
    #     for qf in query_file_list:
    #         entities = analyze_single_query(parse_original_query(qf),prob_table)
    #         for e in entities:
    #             if e not in update_entities:
    #                 update_entities[e] = 1
    #             else:
    #                 update_entities[e] += 1
    #     # update the top 3 entities' value in probability table
    #     print(update_entities)
    #     entities_promote = sorted(update_entities.items(), key=lambda x: x[1], reverse=True)
    #     # print(entities_promote)
    #     top_k_entities_promote = [x[0] for x in entities_promote]
    #     print(top_k_entities_promote)
    #     # print(prob_table)
    #     increase_value = 10
    #     for e in reversed(top_k_entities_promote):
    #         prob_table[e] += increase_value
    #         increase_value += 10
    #     with open(args.prob_table, 'w') as outfile:
    #         json.dump(prob_table, outfile)
    #     print("finish update the prob_table")

        # ------------- analyze all queries, including the non executable ones
        # rule_triggered_file = os.path.join(experiment_folder,"rule_test")
        # arg_list = ["grep -r \"Rule,\" " + experiment_folder + "> temp_rule"]
        # print(arg_list)
        # complete = subprocess.run(arg_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',shell=True)

        # if complete.returncode == 0:
        #     print(" *** succeed:", experiment_folder)
        #     shutil.move("temp_rule",rule_triggered_file)
        # else:
        #     print(" *** failed:",experiment_folder)
        # missed_rule, fired_rule = missed_fired_rule(rule_triggered_file)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='analyze fired rewrite rules')

    # optional:
    # parser.add_argument(
    #     '--verbose', help='print verbose info', action='store_true')
    # whether to analyze missing rule
    parser.add_argument('--missing_rule', help='whether to analyze missing rule', action='store_true')
    parser.add_argument('--rulename', type=str, nargs='?',
                        default='tpchsmall', help='name of rule to find')
    # parser.add_argument('--port', type=int, nargs='?',
    #                     default=5432, help='port of dataase')# required:
    # parser.add_argument('--host', type=str, nargs='?', default='/tmp', help='host of database')

    requiredNamed = parser.add_argument_group('required named arguments')

    # requiredNamed.add_argument('--workers', type=int, nargs='?',
    #                     default=0, help='number of workers',required=True)
    # requiredNamed.add_argument('--dbms', type=str, nargs='?',
    #                     default='.', help='tested_dbms',required=True)
    requiredNamed.add_argument('--output', type=str, nargs='?',
                        default='.', help='a list of experiment folders',required=True)
    requiredNamed.add_argument('--figoutput', type=str, nargs='?',
                        default='.', help='a folder to store figures',required=True)
    requiredNamed.add_argument('--mapname', type=str, nargs='?',
                        default='.', help='heatmap name',required=True)
    requiredNamed.add_argument('--bqfile', type=str, nargs='?',
                        default='.', help='bq file that triggers performance bugs',required=True)
    # requiredNamed.add_argument('--promote', type=int, nargs='?',
    #                     default=0, help='percentage of rules to promote',required=True)
    # requiredNamed.add_argument('--prob_table', type=str, nargs='?',
    #                     default='.', help='prob table to be updated',required=True)
    # requiredNamed.add_argument('--queries', type=int, nargs='?',
    #                     default=1, help='analyzed batch',required=True)
    # requiredNamed.add_argument('--rewriter', type=str, nargs='?', default=".", help='location of rewriter', required=True)
    # requiredNamed.add_argument('--dbname', type=str, nargs='?', default=".", help='large database name', required=True)
    args = parser.parse_args()

    exit(main(args))
