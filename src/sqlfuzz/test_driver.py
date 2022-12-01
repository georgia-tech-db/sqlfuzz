#!/usr/bin/env python3

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
import traceback
from sqlfuzz.validator import *
from sqlfuzz.rule_finder import *
import sqlfuzz.mutator as mutator


def call_calcite(run_folder, index, config_dic, args):
    #
    worker_folder = os.path.join(os.path.abspath(run_folder), str(index))
    if not (os.path.exists(worker_folder)):
        raise Exception(
            "Sorry, query generator does not generate queries in folder",
            worker_folder)
    # after sqlsmith, run query rewrite
    #---------------------------------- run calcite (generate necessary json file) -----------------
    # generate necessary json file for calcite configuration
    # file_name = os.path.join(worker_folder, "input.sql")
    calcite_json = {}
    # info = {}
    # info["log_err"] = "calcite_err"
    # info["input_queries"] = "input.sql"
    # info["output"] = "out"
    # info["rewrite_times"] = 10
    calcite_json["calcite_setting"] = config_dic
    # print(config_dic)
    with open(os.path.join(worker_folder, "calcite_config.json"),
              'w') as outfile:
        json.dump(calcite_json, outfile, indent=4)

    os.chdir(os.path.join(args.rewriter, "core/build/libs"))
    # create folder to store rewritten queries
    rewrite_folder = os.path.join(worker_folder, "out")
    if os.path.exists(rewrite_folder):
        shutil.rmtree(rewrite_folder)
        os.makedirs(rewrite_folder)
    else:
        os.makedirs(rewrite_folder)
    # run calcite
    arg_list = [
        "java -cp calcite-core-1.22.0-SNAPSHOT-tests.jar:./*:. org.apache.calcite.test.Transformer "
        + worker_folder
    ]
    print(arg_list)
    complete = subprocess.run(arg_list,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8',
                              shell=True)
    if complete.returncode == 0:
        # print("finish query rewriter")
        # safe to remove sqlsmith's output
        # os.remove(file_name)
        return 0
    else:
        print(" *** failed:", index, complete.stderr, '***')
        return -1


def call_sqlsmith(index):
    #---------------------------------- set up output directory -----------------
    worker_folder = os.path.join(os.path.abspath(args.output), str(index))
    if os.path.exists(worker_folder):
        shutil.rmtree(worker_folder)
        os.makedirs(worker_folder)
    else:
        os.makedirs(worker_folder)
    file_name = os.path.join(worker_folder, "input.sql")
    #---------------------------------- run sqlsmith -----------------
    arg_list = [
        "sqlsmith --max-queries=" + str(args.queries) +
        " --exclude-catalog --dry-run --target=\"host=/tmp port=5432 dbname=tpchsmall\" 2>"
        + file_name
    ]
    # run the process
    complete = subprocess.run(arg_list,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8',
                              shell=True)

    if complete.returncode == 0:
        print("finished worker sqlsmith:\n", index, complete.stderr)
        return 0
    else:
        print(" *** failed:", index, complete.stderr, '***',
              " Check if postgresql has started")
        exit(1)


def call_sa(run_folder, prob_file, index, conf, args):
    #---------------------------------- set up output directory -----------------
    worker_folder = os.path.join(os.path.abspath(run_folder), str(index))
    os.makedirs(worker_folder)
    file_name = os.path.join(worker_folder, "input.sql")
    #---------------------------------- run sqlsmith -----------------

    log_file_sa = os.path.join(worker_folder, "log_sa" + str(index))

    fuzz_args = mutator.FuzzArgs(strategy="seq", db_info=conf, prob_table = prob_file,
                                 queries=args.queries, stdout=log_file_sa, stderr = file_name)
    try:
        mutator.run_fuzz(fuzz_args)
    except Exception as e:
        print(" *** failed:", index, '***',
              " Check log file" + log_file_sa)
        exit(1)


    # arg_list = [
    #     "~/miniconda3/bin/python ./sqlfuzz/mutator.py --prob_table=" + prob_file +
    #     " --db_info=" + conf +" -s seq --queries " + str(args.queries) +
    #     " 1>" + log_file_sa + " 2>" + file_name
    # ]
    # print(arg_list)
    # # arg_list = ["sqlsmith --max-queries="+ str(args.queries)+" --exclude-catalog --dry-run --target=\"host=/tmp port=5432 dbname=tpchsmall\" 2>"+file_name]
    # # run the process
    # complete = subprocess.run(arg_list,
    #                           stdout=subprocess.PIPE,
    #                           stderr=subprocess.PIPE,
    #                           encoding='utf-8',
    #                           shell=True)

    # if complete.returncode == 0:
    #     return 0
    # else:
    #     print(" *** failed:", index, complete.stderr, '***',
    #           " Check log file" + log_file_sa)
    #     exit(1)


def postgresql_execution(filename, databasename):
    conn = psycopg2.connect(
        "dbname=tpchsmall user=" + args.user +
        " connect_timeout=30 host=/tmp options='-c statement_timeout=500000'")
    conn.autocommit = True
    with open(filename) as f:
        s = " ".join([x.strip() for x in f])

    queries = s.split(";")
    result_list = []
    success_execution = 0
    # track how many semantically equivalent queries get executed
    for i in range(len(queries) - 1):
        if ("insert" in queries[i] or "INSERT" in queries[i]):
            return 1 - len(queries), len(queries) - 1
        if ("--" in queries[i]):
            continue
        cur = conn.cursor()
        try:
            cur.execute(queries[i] + ";")
            result = cur.fetchall()
            result_list.append(len(result))
            cur.close()
        except psycopg2.OperationalError as error:
            print("timeout error", error, filename, i)
            success_execution -= 1
            cur.close()
        except Exception as error:
            print("Oops! An exception has occured:", error, filename, i)
            success_execution -= 1
            cur.close()

    a = dict(Counter(result_list))
    # report potential correctness bug
    if (len(a) != 1 and len(a) != 0):
        with open(log_file_name, 'a+') as log:
            log.write("inconsistent output: %s\n" % filename)
            for listitem in result_list:
                log.write('%s\t' % listitem)
            log.write('\n')
    # close the communication with the PostgreSQL
    elif (len(result_list) > 0 and result_list[0] > 1):
        # it means all queries have the same returned result
        # execute explain verison to see their cost
        cost_result_list = []
        for i in range(len(queries) - 1):
            if ("insert" in queries[i] or "INSERT" in queries[i]):
                return 1 - len(queries), len(queries) - 1
            if ("--" in queries[i]):
                continue
            clear_cache()
            conn = psycopg2.connect(
                "dbname=" + "tpchsmall" + " user=" + args.user +
                " connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
            )

            cur = conn.cursor()
            cur = conn.cursor()
            try:
                cur.execute("explain " + queries[i] + ";")
                first_line_result = str(cur.fetchone()[0])
                start = "cost="
                end = ' rows'
                cost_result = re.search('%s(.*)%s' % (start, end),
                                        first_line_result).group(1)
                max_result = cost_result.split("..")[1]
                cost_result_list.append(float(max_result))
                cur.close()
            except psycopg2.OperationalError as error:
                print("timeout error", error, filename, i)
                cur.close()
            except Exception as error:
                print("Oops! An exception has occured:", error, filename, i)
                cur.close()
        est_cost_dict = dict(Counter(cost_result_list))
        if len(cost_result_list) > 0 and (max(cost_result_list) >
                                          2 * min(cost_result_list)):
            print("s\n")
            with open(log_file_name, 'a+') as log:
                log.write("query name: %s\n" % filename)
                for listitem in cost_result_list:
                    log.write('%s\t' % listitem)
                log.write('\n')
        # print(est_cost_dict,filename, file=log_file_name)

    return success_execution, len(queries) - 1


def clear_cache():
    arg_list = ["stop_pg13.sh ; start_pg13.sh"]
    # run the process
    complete = subprocess.run(arg_list,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8',
                              shell=True)

    if complete.returncode == 0:
        # pass
        print("resetting cache successful\n", complete.stderr)
        # return 0
    else:
        print("resetting cache failure\n", complete.stderr)


def clear_cache_cockroachdb():
    arg_list = ["stop_cockroach.sh ; start_cockroach.sh"]
    # run the process
    complete = subprocess.run(arg_list,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8',
                              shell=True)

    if complete.returncode == 0:
        # pass
        print("resetting cache successful\n", complete.stderr)
        # return 0
    else:
        print("resetting cache failure\n", complete.stderr)


def postgresql_execution_large(databasename):
    final_inspection_folder = os.path.join(os.path.abspath(args.output),
                                           "final_inspection")
    if not os.path.exists(final_inspection_folder):
        os.makedirs(final_inspection_folder)
    with open(os.path.join(args.output, "log_driver.txt"), "r+") as log:
        lines = log.readlines()
        for line in lines:
            if "query name" in line:
                filename = (line.split(":")[1].split()[0])
                with open(filename) as f:
                    s = " ".join([x.strip() for x in f])
                    queries = s.split(";")
                    execution_time_list = []
                    query_list = []
                    for i in range(len(queries) - 1):
                        if ("--" in queries[i]):
                            continue
                        clear_cache()
                        conn = psycopg2.connect(
                            "dbname=" + databasename + " user=" + args.user +
                            " connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
                        )

                        cur = conn.cursor()
                        try:
                            cur.execute("EXPLAIN (ANALYZE, TIMING OFF) " +
                                        queries[i] + ";")
                            records = cur.fetchall()
                            last_line = (records[len(records) - 1][0])
                            time_result = re.search(
                                '%s(.*)%s' % ("Time: ", " ms"),
                                last_line).group(1)
                            execution_time_list.append(float(time_result))
                            cur.close()
                            print("l")
                            query_list.append(queries[i])
                        except psycopg2.OperationalError as error:
                            print("timeout error", error, filename, i)
                            cur.close()
                        except Exception as error:
                            print("Oops! An exception has occured:", error,
                                  filename, i)
                            cur.close()
                    if len(execution_time_list) > 0 and (
                            max(execution_time_list) >
                            float(1.5) * min(execution_time_list)):
                        # run 3 more times to make sure the time difference is consistent
                        vote = 0
                        for i in range(3):
                            _execution_time_list = []
                            for q in query_list:
                                clear_cache()
                                conn = psycopg2.connect(
                                    "dbname=" + databasename + " user=" +
                                    args.user +
                                    " connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
                                )
                                cur = conn.cursor()
                                try:
                                    cur.execute(
                                        "EXPLAIN (ANALYZE, TIMING OFF) " + q +
                                        ";")
                                    records = cur.fetchall()
                                    last_line = (records[len(records) - 1][0])
                                    time_result = re.search(
                                        '%s(.*)%s' % ("Time: ", " ms"),
                                        last_line).group(1)
                                    _execution_time_list.append(
                                        float(time_result))
                                    cur.close()
                                except psycopg2.OperationalError as error:
                                    print("timeout error", error, filename, i)
                                    cur.close()
                                except Exception as error:
                                    print("Oops! An exception has occured:",
                                          error, filename, i)
                                    cur.close()
                            if (max(_execution_time_list) >
                                    float(1.5) * min(_execution_time_list)):
                                vote += 1
                        if vote >= 2:
                            log.write(
                                "final query that needs manual inspection: %s %f %f\n"
                                % (filename, max(execution_time_list),
                                   min(execution_time_list)))
                            print("find suspicious queries")
                            for listitem in execution_time_list:
                                log.write('%s\t' % listitem)
                            log.write('\n')
                            try:
                                print("find!")
                                shutil.copy(
                                    filename,
                                    os.path.join(final_inspection_folder,
                                                 filename.replace("/", "_")))
                            except Exception as error:
                                print("Oops! An exception has occured:", error)


def cockroachdb_execution_large(databasename):
    dsn = 'postgresql://root@localhost:26257/tpch5?sslmode=disable'
    final_inspection_folder = os.path.join(os.path.abspath(args.output),
                                           "final_inspection")
    if not os.path.exists(final_inspection_folder):
        os.makedirs(final_inspection_folder)
    with open(os.path.join(args.output, "log_driver.txt"), "r+") as log:
        lines = log.readlines()
        for line in lines:
            if "cockroachquery name" in line:
                filename = (line.split(":")[1].split()[0])
                with open(filename) as f:
                    s = " ".join([x.strip() for x in f])
                    queries = s.split(";")
                    execution_time_list = []
                    query_list = []
                    for i in range(len(queries) - 1):
                        if ("--" in queries[i]):
                            continue
                        conn = psycopg2.connect(dsn, connect_timeout=30)
                        cur = conn.cursor()
                        try:
                            cur.execute("set statement_timeout=500000;")
                            start_time = time.time()
                            cur.execute(queries[i] + ";")
                            time_result = time.time() - start_time
                            execution_time_list.append(float(time_result))
                            cur.close()
                            print("l")
                            query_list.append(queries[i])
                        except psycopg2.OperationalError as error:
                            print("timeout error", error, filename, i)
                            cur.close()
                        except Exception as error:
                            print("Oops! An exception has occured:", error,
                                  filename, i)
                            cur.close()
                    if len(execution_time_list) > 0 and (
                            max(execution_time_list) >
                            float(1.5) * min(execution_time_list)):
                        # run 3 more times to make sure the time difference is consistent
                        print("serious validation", line)
                        vote = 0
                        for i in range(5):
                            _execution_time_list = []
                            random.shuffle(query_list)
                            for q in query_list:
                                conn = psycopg2.connect(dsn,
                                                        connect_timeout=30)
                                cur = conn.cursor()
                                try:
                                    cur.execute(
                                        "set statement_timeout=500000;")
                                    start_time = time.time()
                                    cur.execute(q + ";")
                                    time_result = time.time() - start_time
                                    _execution_time_list.append(
                                        float(time_result))
                                    cur.close()
                                except psycopg2.OperationalError as error:
                                    print("timeout error", error, filename, i)
                                    cur.close()
                                except Exception as error:
                                    print("Oops! An exception has occured:",
                                          error, filename, i)
                                    cur.close()
                            if (max(_execution_time_list) >
                                    float(1.5) * min(_execution_time_list)):
                                vote += 1
                        if vote >= 3:
                            log.write(
                                "final query that needs manual inspection: %s %f %f\n"
                                % (filename, max(execution_time_list),
                                   min(execution_time_list)))
                            print("find suspicious queries")
                            for listitem in execution_time_list:
                                log.write('%s\t' % listitem)
                            log.write('\n')
                            try:
                                print("find!")
                                shutil.copy(
                                    filename,
                                    os.path.join(final_inspection_folder,
                                                 filename.replace("/", "_")))
                            except Exception as error:
                                print("Oops! An exception has occured:", error)


def validate_equivalent_query_sets(filename, executor):
    with open(filename) as f:
        s = " ".join([x.strip() for x in f])
    queries = s.split(";")
    executor.run_queries_time(queries, filename)


def cockroach_execution_small(filename, executor):
    with open(filename) as f:
        s = " ".join([x.strip() for x in f])
    queries = s.split(";")

    # wrapper for running for correctness and plan diff
    executor.run_queries(queries, filename)
    # if (executable_queries is not None):
    #     executor.run_queries_cost(executable_queries, filename)
    return None


def cockroach_execution(filename, databasename):
    #1. bail out if "explain"'s plan rows are the same (heuristics)
    #2. report correctness issue
    #3. run at least 3 times to report performance issue
    dsn = 'postgresql://root@localhost:26257/tpch1?sslmode=disable'
    conn = psycopg2.connect(dsn, connect_timeout=30)
    # conn = psycopg2.connect(
    #     database='tpch1',
    #     user='root',
    #     sslmode='require',
    #     sslrootcert='certs/ca.crt',
    #     sslkey='certs/client.root.key',
    #     sslcert='certs/client.root.crt',
    #     port=26257,
    #     host='localhost')
    conn.autocommit = True
    print("execute query filename:", filename)
    with open(filename) as f:
        s = " ".join([x.strip() for x in f])

    queries = s.split(";")
    explain_result_list = []
    success_execution = 0
    query_list = []
    result_list = []
    #------------------check correctness---------------
    for i in range(len(queries) - 1):
        if ("insert" in queries[i] or "INSERT" in queries[i]):
            return 1 - len(queries), len(queries) - 1
        if ("--" in queries[i]):
            continue
        cur = conn.cursor()
        try:
            cur.execute("set statement_timeout=5000;")
            cur.execute(queries[i] + ";")
            result = cur.fetchall()
            result_list.append(len(result))
            query_list.append(queries[i])
            cur.close()
        except psycopg2.OperationalError as error:
            with open(log_file_name, 'a+') as log:
                log.write(
                    "operational error by psycopg %s filename %s query name: %s \n"
                    % (error, filename, queries[i]))
                log.write('\n')
                cur.close()
        except Exception as error:
            print("Oops! An exception threw by DBMS:", error, filename, i)
            success_execution -= 1
            cur.close()

    a = dict(Counter(result_list))
    # report potential correctness bug
    if (len(a) != 1 and len(a) != 0):
        with open(log_file_name, 'a+') as log:
            log.write("inconsistent output: %s\n" % filename)
            for listitem in result_list:
                log.write('%s\t' % listitem)
            log.write('\n')
    #------------------if output the same, check estimate cost-------------
    elif (len(result_list) > 0 and result_list[0] > 1):
        # result is the same
        cost_result_list = []
        for query in query_list:
            # clear_cache_cockroachdb()
            conn = psycopg2.connect(dsn, connect_timeout=30)
            conn.autocommit = True
            cur = conn.cursor()
            try:
                cur.execute("set statement_timeout=5000;")
                cur.execute("EXPLAIN (OPT, VERBOSE) " + query + ";")
                for row in cur.fetchall():
                    if len(row) > 0 and "cost: " in row[0]:
                        estimate_cost = float(row[0].split("cost: ")[1])
                        # print(estimate_cost)
                        cost_result_list.append(estimate_cost)
                        break
                cur.close()
            except psycopg2.OperationalError as error:
                with open(log_file_name, 'a+') as log:
                    log.write(
                        "operational error %s filename %s query name: %s \n" %
                        (error, filename, query))
                    log.write('\n')
                    cur.close()
            except Exception as error:
                print("Oops! An exception has occured:", error, filename)
                traceback.print_exc()
                cur.close()
        if len(cost_result_list) > 0 and (max(cost_result_list) >
                                          min(cost_result_list)):
            print("s\n")
            with open(log_file_name, 'a+') as log:
                log.write("cockroachquery name: %s\n" % filename)
                for listitem in cost_result_list:
                    log.write('%s\t' % listitem)
                log.write('\n')

        # --------------------------Inspect performance (5 iterations)------------------
        # vote=0
        # execution_time_list=[]
        # for i in range (5):
        #     _execution_time_list = []
        #     for q in query_list:
        #         # clear_cache_cockroachdb()
        #         conn = psycopg2.connect(dsn, connect_timeout=30)
        #         conn.autocommit = True
        #         cur = conn.cursor()
        #         try:
        #             start_time=time.time()
        #             cur.execute(q +";")
        #             # records = cur.fetchall()
        #             time_result = time.time() - start_time
        #             print(time_result)
        #             _execution_time_list.append(float(time_result))
        #             cur.close()
        #         except psycopg2.OperationalError as error:
        #             print("timeout error", error, filename,i)
        #             cur.close()
        #         except Exception as error:
        #             print ("Oops! An exception has occured:", error,filename, i)
        #             cur.close()
        #     if (max(_execution_time_list) > float(1.5) * min(_execution_time_list)):
        #         vote += 1
        #         execution_time_list = _execution_time_list
        # if vote >= 3:
        #     with open(log_file_name,'a+') as log:
        #         log.write("query name: %s\n" % filename)
        #         for listitem in execution_time_list:
        #             log.write('%s\t' % listitem)
        #         log.write('\n')

        # try:
        #     print("find!")
        #     shutil.copy(filename, os.path.join(final_inspection_folder, filename.replace("/", "_")))
        # except Exception as error:
        #     print ("Oops! An exception has occured:", error)

    else:
        # it is very likely they are all the same plan, so no deed to execute the query
        pass

        # if (max(cost_result_list) > 2 * min(cost_result_list)):
        #     print("s\n")
        #     with open(log_file_name,'a+') as log:
        #         log.write("query name: %s\n" % filename)
        #         for listitem in cost_result_list:
        #             log.write('%s\t' % listitem)
        #         log.write('\n')
        # print(est_cost_dict,filename, file=log_file_name)

    return success_execution, int((len(queries) - 1) / 2)
    # track how many semantically equivalent queries get executed





def bootstrap_run(sub_folder_name, global_prob_table):
    # create folder
    os.mkdir(sub_folder_name)
    # move prob_table
    post_fix = sub_folder_name.split("/")[-1]
    local_prob_table = os.path.join(sub_folder_name,
                                    "prob_table_" + str(post_fix) + ".json")
    try:
        copyfile(global_prob_table, local_prob_table)
    except Exception as inst:
        print("exception in copy initial prob_table")
    # log file
    return os.path.abspath(local_prob_table)


def extract_entities_given_query_list(old_prob_file, query_file_list):
    with open(old_prob_file) as f:
        print("Try read probability table", f.name)
        prob_table = json.load(f)
    update_entities = {}
    for qf in query_file_list:
        original_query = parse_original_query(qf)
        if original_query is None:
            continue
        entities = analyze_single_query(original_query, prob_table)
        for e in entities:
            if e not in update_entities:
                update_entities[e] = 1
            else:
                update_entities[e] += 1
    return list(update_entities.keys())


def update_prob_given_entities(old_prob_file, update_entities):
    with open(old_prob_file) as f:
        print("Try read probability table", f.name)
        prob_table = json.load(f)
    print(update_entities)
    # update all interesting entities in probability table

    # decrease simple: join appear but simple not
    if ("left" in update_entities or "full" in update_entities
            or "inner" in update_entities):
        if ("simple" not in update_entities):
            prob_table["simple"] -= 30
        else:
            # keep simple same: join appear simple appear
            prob_table["simple"] -= 15

    else:
        # increase simple: simple appear but join not
        if ("simple" in update_entities):
            prob_table["simple"] += 30

    if ("left" in update_entities or "full" in update_entities
            or "inner" in update_entities):
        # update join condition entities
        if ("true" in update_entities):
            if ("bool_expr_join" not in update_entities):
                # increase true: only true not condition
                prob_table["true"] += 30
            else:
                # keep true the same: true and condition both appear
                pass
        else:
            if ("bool_expr_join" in update_entities):
                # decrease true: only condition appear
                prob_table["true"] -= 30
        if ("true" in update_entities and prob_table["true"] > 800):
            update_entities.remove("true")
            prob_table["true"] = 500
        if ("bool_expr_join" in update_entities and prob_table["true"] < 200):
            update_entities.remove("bool_expr_join")
            prob_table["true"] = 500

        # update join type entities
        if ("left" in update_entities):
            if ("full" in update_entities):
                if ("inner" in update_entities):
                    # all types interesting, not update
                    pass
                else:
                    # all except inner interesing, update
                    prob_table["inner"] -= 30
                    prob_table["full"] += 15
                    prob_table["left"] += 15
            else:
                if ("inner" in update_entities):
                    # all except full interesing, update
                    prob_table["inner"] += 15
                    prob_table["full"] -= 30
                    prob_table["left"] += 15
                    pass
                else:
                    # only left interesing, update
                    prob_table["inner"] -= 15
                    prob_table["full"] -= 15
                    prob_table["left"] += 30
        else:
            if ("full" in update_entities):
                if ("inner" in update_entities):
                    # all types except left interesting
                    prob_table["left"] -= 30
                    prob_table["full"] += 15
                    prob_table["inner"] += 15
                else:
                    # only full interesting
                    prob_table["inner"] -= 15
                    prob_table["full"] += 30
                    prob_table["left"] -= 15
            else:
                if ("inner" in update_entities):
                    # only inner interesting
                    prob_table["inner"] += 30
                    prob_table["full"] -= 15
                    prob_table["left"] -= 15
                else:
                    pass

    if "simple" in update_entities:
        update_entities.remove("simple")
        if (prob_table["simple"] > 1000 or prob_table["simple"] < 0):
            prob_table["simple"] = 300
    if ("left" in update_entities):
        update_entities.remove("left")
    if ("full" in update_entities):
        update_entities.remove("full")
    if ("inner" in update_entities):
        update_entities.remove("inner")
    if ("bool_expr_join" in update_entities):
        update_entities.remove("bool_expr_join")
    join_entities = ["left", "full", "inner"]
    for j in join_entities:
        if prob_table[j] < 0 or prob_table[j] > 1000:
            # reset to default
            for j_ in join_entities:
                prob_table[j_] = 333
    print("normal entites", update_entities)
    for k in update_entities:
        if (prob_table[k] < 600):
            prob_table[k] += 30
        else:
            prob_table[k] = 200

    with open(old_prob_file, 'w') as outfile:
        json.dump(prob_table, outfile)


# def update_prob_given_query_list(old_prob_file, query_file_list):
#     return
#     with open(old_prob_file) as f:
#         print("Try read probability table", f.name)
#         prob_table = json.load(f)
#     update_entities = {}
#     for qf in query_file_list:
#         original_query = parse_original_query(qf)
#         if original_query is None:
#             continue
#         entities = analyze_single_query(original_query, prob_table)
#         for e in entities:
#             if e not in update_entities:
#                 update_entities[e] = 1
#             else:
#                 update_entities[e] += 1


#     with open(old_prob_file, 'w') as outfile:
#         json.dump(prob_table, outfile)



# def prob_table_updator_perfbug(old_prob_file, query_file_list):
#     new_prob_table = update_prob_given_query_list(old_prob_file,
#                                                   query_file_list)
#     with open(old_prob_file, 'w') as outfile:
#         json.dump(new_prob_table, outfile)

def extract_queryfilelist_mutator(executable_query_file):
    fired_rules_dict = {}
    with open(executable_query_file) as f:
        lines = f.readlines()
        for l in lines:
            fired_rules_dict = process_single_query_file(
                l.strip('\n'), fired_rules_dict)
    print("---------------analyze dictionary-------------------")
    print("accumulative fired rules: ", len(fired_rules_dict))
    fired_rules_list = []
    rules_frequency = []
    for k, v in fired_rules_dict.items():
        fired_rules_list.append(v.rule_name)
        rules_frequency.append(v.frequency)
    fired_rules_list.sort()
    rules_frequency.sort()
    promote = 10
    bottom_k_rules_frequency = rules_frequency[:int(promote *
                                                    len(rules_frequency) /
                                                    100)]
    print(bottom_k_rules_frequency)
    query_file_list = []
    for k, v in fired_rules_dict.items():
        # print(k)
        if v.frequency in bottom_k_rules_frequency:
            query_file_list += (v.query_list)
    # print(query_file_list)
    query_file_list = list(set(query_file_list))
    return query_file_list



def main(args):
    exit_code = 1
    # the number of parallel workers
    exp_start_time = time.time()
    this_dir, _ = os.path.split(__file__)
    CONF_ROOT = os.path.join(this_dir, "amoeba_conf")
    args.rewriter = os.path.join(this_dir, args.rewriter)

    workers = args.workers
    if (workers == 0):
        print("must have at least 1 working process")
        return 1
    global log_file_name
    global valid_query_num
    global nonempty_query_num
    existing_queries = False
    if (args.existingqueries != "."):
        existing_queries = True
        print("validate existing equivalent queries")
    small_executor_list = []
    large_executor_list = []
    # args.output is a root folder for this experiment
    try:
        os.makedirs(args.output)
    except Exception as e:
        print("exception when creating top folder for experiment", e)
        shutil.rmtree(args.output)
        os.makedirs(args.output)
        # traceback.print_exc()
    global_prob_table = os.path.join(args.output, "prob_table.json")
    log_file_name = os.path.join(args.output, "log_driver.txt")
    triage_file_name = os.path.join(args.output, "bugs.md")
    conf_file = json.load(open(args.dbconf))
    calcite_conf_dic = json.load(open(os.path.join(CONF_ROOT, "calcite_config.json")))
    dbname = conf_file["name"]
    try:
        copyfile(os.path.join(CONF_ROOT, "prob_table.json"), global_prob_table)
    except Exception as inst:
        print("exception in copy initial prob_table")
    with open(log_file_name, 'w') as fp:
        fp.close()
    with open(triage_file_name, 'w') as fp:
        fp.close()

    # -----------each run of tool chain----------------------
    for run in range(args.num_loops + 1):
        print("run %d" % run)
        with open(log_file_name, 'a+') as log:
            now = datetime.now()
            log.write("start time: %s\n" % str(now))
        # --------------------entire run of tool chain-------------------------
        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        current_time = current_time.replace(':', '')
        run_folder_name = os.path.abspath(
            os.path.join(args.output, current_time))
        run_prob_table = bootstrap_run(run_folder_name, global_prob_table)

        if (existing_queries is False):
            # ------ generator -----
            print("start query generator")
            with concurrent.futures.ProcessPoolExecutor() as executor:
                for i in range(workers):
                    # future = executor.submit(call_sqlsmith, i)
                    future = executor.submit(call_sa, run_folder_name,
                                             run_prob_table, i, args.dbconf, args)
            print("finish query generator")

            # ------ rewriter -----
            print("start query rewriter")
            with concurrent.futures.ProcessPoolExecutor() as executor:
                for i in range(workers):
                    future = executor.submit(call_calcite, run_folder_name, i, calcite_conf_dic, args)
            print("finish query rewriter")
        else:
            # validate a given set of equivalent queries
            pass

        # ------ validator -----
        if (args.validate):
            print("start validator")
            try:
                if (existing_queries is True):
                    # calcite existing queries experiment
                    e_small = Executor(args.dbms, "small", run)
                    e_small.timeout = args.query_timeout
                    e_small.bootstrap_dbms_benchexp_large(dbname)
                    e_small.error_file = triage_file_name
                else:
                    if ("demo" not in dbname):
                        # tpch experiment
                        e_small = Executor(args.dbms, "small", run)
                        e_small.timeout = args.query_timeout
                        e_small.bootstrap_dbms()
                        e_small.error_file = triage_file_name
                    else:
                        # demo experiment
                        e_small = Executor(args.dbms, "small", run)
                        e_small.timeout = args.query_timeout
                        e_small.bootstrap_dbms_benchexp_large(dbname)
                        e_small.error_file = triage_file_name
            except Exception as inst:
                traceback.print_exc()
                print(inst)
            print("begin compare plan cost of equivalent queries")

            for i in range(workers):
                if (existing_queries is False):
                    worker_folder = os.path.join(run_folder_name, str(i))
                    rewrite_folder = os.path.join(worker_folder, "out")
                else:
                    rewrite_folder = args.existingqueries
                arr_names = os.listdir(rewrite_folder)
                for i in range(len(arr_names)):
                    # run each set of queries
                    filename = os.path.join(rewrite_folder, arr_names[i])
                    # new function that takes care of all below stuff
                    # print("run small executor with query", filename)
                    cockroach_execution_small(filename, e_small)
                    if i != 1 and i % 500 == 1 and args.dbms == "postgresql":
                        print(
                            "restart server free some RAM"
                        )
                        clear_cache()
                        print("finish restart server")
                    if i != 1 and i % 300 == 1 and args.dbms == "cockroachdb":
                        print(
                            "restart server free some RAM"
                        )
                        clear_cache_cockroachdb()
                        print("finish restart server")

            small_executor_list.append(e_small)
            # ------- write to executable query file ------
            executable_query_file = os.path.join(run_folder_name,
                                                 "executable_queries.txt")
            with open(executable_query_file, 'w') as fp:
                # the executable query file store all executable queries to date
                for e in small_executor_list:
                    for q in e.get_executable_query_file():
                        fp.write("%s\n" % q)
            print("start comparing runtime performance of equivalent queries")
            try:
                if (existing_queries is True):
                    # calcite existing queries experiment
                    e_large = Executor(args.dbms, "large", run)
                    e_large.error_file = triage_file_name
                    e_large.timeout = args.query_timeout
                    e_large.bootstrap_dbms_benchexp_large(dbname)

                else:
                    if ("demo" not in dbname):
                        # tpch experiment
                        e_large = Executor(args.dbms, "large", run)
                        e_large.error_file = triage_file_name
                        e_large.timeout = args.query_timeout
                        e_large.bootstrap_dbms()
                    else:
                        e_large = Executor(args.dbms, "large", run)
                        e_large.error_file = triage_file_name
                        e_large.timeout = args.query_timeout
                        e_large.bootstrap_dbms_benchexp_large(dbname)
                        # print(e_large.run_cmd)


            except Exception as inst:
                traceback.print_exc()
                print(inst)

            if (args.boring):
                print("execute all executable queries")
                interesting_perf_query_file = e_small.get_executable_query_file(
                )
            else:
                interesting_perf_query_file = e_small.get_interesting_perf_queries(
                )
            for i in range(len(interesting_perf_query_file)):
                print(interesting_perf_query_file[i])
                validate_equivalent_query_sets(interesting_perf_query_file[i],
                                             e_large)

            large_executor_list.append(e_large)

            if args.num_loops == 0:
                pass
            elif run < args.num_loops:
                print("Use feedback to update the prob_table.json")
                # this function will rewrite (update) global_prob_table
                if (args.feedback == "validator"):
                    extract_entities = extract_entities_given_query_list(global_prob_table, e_large.get_final_perf_queries())
                    update_prob_given_entities(global_prob_table, extract_entities)
                    print(
                        "finish update the prob_table.json from validated perf bug feedback"
                    )

                if (args.feedback == "mutator"):
                    mutator_query_list = extract_queryfilelist_mutator(executable_query_file)
                    extract_entities = extract_entities_given_query_list(global_prob_table, mutator_query_list)
                    update_prob_given_entities(global_prob_table, extract_entities)
                    print(
                        "finish update the prob_table.json from mutator feedback"
                    )

                if (args.feedback == "." or args.feedback == "none"):
                    print("does not update prob_table.json")

                if (args.feedback == "both"):
                    mutator_query_list = extract_queryfilelist_mutator(executable_query_file)
                    extract_entities_mutator = extract_entities_given_query_list(global_prob_table, mutator_query_list)
                    extract_entities_validator = extract_entities_given_query_list(global_prob_table, e_large.get_final_perf_queries())
                    total_entities = list(set(extract_entities_mutator + extract_entities_validator))
                    update_prob_given_entities(global_prob_table, total_entities)
                    # print(
                    #     "finish update the prob_table.json from validated perf bug feedback"
                    # )
                    # prob_table_updator_rewrite(global_prob_table,
                    #                            executable_query_file)
                    # print(
                    #     "finish update the prob_table.json from mutator feedback"
                    # )

            if (run == args.num_loops):
                # store executable query file to the top folder
                all_executable_queries = os.path.join(
                    args.output, "executable_queries.txt")
                copyfile(executable_query_file, all_executable_queries)
                # analyze total fired rule information

    # -------------- finish all runs----------------
    elapsed_time = time.time() - exp_start_time
    # print("start time: %f" % float(exp_start_time))
    print("elapsed time: %f" % float(elapsed_time))
    exec_query_num = 0
    perf_diff_query_num = 0
    for e in small_executor_list:
        exec_query_num += len(e.get_executable_query_file())
    print("finish all runs, total distinct executable queries: ",
          exec_query_num)
    for e in large_executor_list:
        perf_diff_query_num += len(e.get_final_perf_queries())
    print("finish all runs, total performance bugs found: ",
          perf_diff_query_num)

    return 1

    if (args.dbms == "postgresql"):
        print("tested dbms is postgresql")
    elif (args.dbms == "cockroachdb"):
        print("tested dbms is cockroach")
    else:
        raise Exception("sorry, the tested dbms", args.dbms,
                        "is not supported yet")

    return exit_code

class FuzzArgs:
    def __init__(self, output, dbconf, port = 5432, workers=1, queries=200, dbms="postgresql"):
        self.output = output
        self.dbconf = dbconf
        self.port = port
        self.workers = workers
        self.queries = queries
        self.dbms = dbms
        self.rewriter = "calcite-fuzzing"
        self.validate = True
        self.num_loops = 100
        self.feedback = None
        self.query_timeout = 30
        self.verbose = True
        self.existingqueries = "."
        self.boring = True


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='options for launching AMOEBA')

    # optional:
    parser.add_argument('--verbose',
                        help='print verbose info',
                        action='store_true')

    parser.add_argument('--port',
                        type=int,
                        nargs='?',
                        default=5432,
                        help='port of dataase')
    # parser.add_argument('--host',
    #                     type=str,
    #                     nargs='?',
    #                     default='/tmp',
    #                     help='host of database')
    parser.add_argument('--num_loops',
                        type=int,
                        nargs='?',
                        default=0,
                        help='number of feedback loops')
    parser.add_argument('--feedback',
                        type=str,
                        nargs='?',
                        default='.',
                        help='what types of feedbacks to utilize')
    parser.add_argument('--existingqueries',
                        type=str,
                        nargs='?',
                        default='.',
                        help='invoke validator on existing equivalent queries')
    parser.add_argument('--validate',
                        help='whether to invoke the VALIDATOR after generating the equivalent query pairs',
                        action='store_true')
    parser.add_argument('--boring',
                        help='whether to compare runtime performance of all executable queries',
                        action='store_true')

    requiredNamed = parser.add_argument_group('required named arguments')

    requiredNamed.add_argument('--workers',
                               type=int,
                               nargs='?',
                               default=0,
                               help='number of parallel workers to invoke GENERATOR and MUTATOR',
                               required=True)
    requiredNamed.add_argument('--dbms',
                               type=str,
                               nargs='?',
                               default='.',
                               help='DBMS that AMOEBA will evaluate on',
                               required=True)
    requiredNamed.add_argument('--output',
                               type=str,
                               nargs='?',
                               default='.',
                               help='output directory',
                               required=True)
    requiredNamed.add_argument('--dbconf',
                               type=str,
                               nargs='?',
                               default='.',
                               help='confiure file to tell the schema for query generation procedure',
                               required=True)
    requiredNamed.add_argument('--queries',
                               type=int,
                               nargs='?',
                               default=1,
                               help='max queries per sqlsmith',
                               required=True)
    requiredNamed.add_argument('--query_timeout',
                               type=int,
                               nargs='?',
                               default=1,
                               help='timeout for executing each query (unit is seconds)',
                               required=True)
    requiredNamed.add_argument('--rewriter',
                               type=str,
                               nargs='?',
                               default=".",
                               help='location of rewriter',
                               required=True)

    args = parser.parse_args()

    exit(main(args))
