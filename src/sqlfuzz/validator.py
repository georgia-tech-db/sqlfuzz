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
from sqlfuzz.error import *
from datetime import datetime
import filecmp

white_list_errors = ["timeout", "syntax", "unknown", "comparison"]
def get_meaninful_min_time(time_list):
    # get the smallest value larger than 0, bc failed execution time is 0
    s = set(time_list)
    if sorted(s)[0] > 0 or len(s)==1:
        return sorted(s)[0]
    return sorted(s)[1]
def calctime(lines, header_idx, footer_idx, result_start, result_end):
    header = lines[header_idx]
    footer = lines[footer_idx]
    # print(header, footer)
    if "-" not in str(header) or "-" not in str(footer):
        print("wrong")
        elapsed = -1.0
        result = "ERROR" + footer + "\n"
    else:
        elapsed = _calctime(header, footer)
        result = (lines[result_start:result_end])

    return elapsed, result
def _calctime(header, footer):
    htime = header.split("+")[0][:-3]
    ftime = footer.split("+")[0][:-3]

    # print(htime, ftime)
    try:
        hdtime = (datetime.strptime(htime, "%Y-%m-%d %H:%M:%S.%f").timestamp())
        fdtime = (datetime.strptime(ftime, "%Y-%m-%d %H:%M:%S.%f").timestamp())

    except Exception as inst:
        print(inst)
        fdtime = 0.0
        hdtime = 1.0
    # print(fdtime, hdtime)
    elapsed = fdtime-hdtime

    return elapsed
def parse_cost_cockroach(output):
    # print("explain result outputis", output)
    for row in output:
        # print(row)
        if len(row) > 0 and "cost: " in row:
            # print("parseed row is", row)
            estimate_cost = float(row.split("cost: ")[1].rstrip("\""))
            # print("estiamte cost ", estimate_cost)
            return estimate_cost
def parse_cost_postgres(output):
    first_line_result = output[0]
    start = "cost="
    end = ' rows'
    cost_result = re.search('%s(.*)%s' % (start, end),first_line_result).group(1)
    estimate_cost = cost_result.split("..")[1]
    return estimate_cost
class Executor(object):
    # responsible executing real queries, analyze results and triage bugs
    # set up 1 for each datebase
    # what to be done for a new DBMS:
    # 1. bootstrap()
    # 2. run_query_()
    # 3. run_queries_cost()
    # 4. parse_cost_()
    def __init__(self, dbms_name, scale, index, stmt_timeout=6, thresh=1.8):
        self.dbms = dbms_name
        self.scale = scale
        self.dsn = None
        self.conn = None
        self.timeout = stmt_timeout
        self.final_inspection_folder = None
        self.error_file = None
        self.bug_idx = 0
        self.diff_threshold = thresh
        self.run_cmd = None
        self.tmpfile = "/tmp/q.sql"
        # this is for running on large database and compare execution time
        self.perf_query_file = []
        # this is for feedback loop
        self.executable_query_file = []
        # this is for bug report and analysisi
        self.validated_perf_query_file = []
        self.idx = index

    def get_interesting_perf_queries(self):
        return self.perf_query_file

    def get_final_perf_queries(self):
        return self.validated_perf_query_file

    def get_executable_query_file(self):
        return self.executable_query_file

    def bootstrap_dbms(self):
        if (self.scale == "small" and self.dbms == "cockroachdb"):
            self.run_cmd = "timeout %d cockroach sql --insecure --host=localhost --port=26257 --database=tpch1 < %s" % (self.timeout, self.tmpfile)
            self.dsn = 'postgresql://root@localhost:26257/tpch1?sslmode=disable'
            # self.conn = psycopg2.connect(self.dsn, connect_timeout=30)
            # self.conn.autocommit = True
            return
        if (self.scale == "large" and self.dbms == "cockroachdb"):
            self.run_cmd = "timeout %d cockroach sql --insecure --host=localhost --port=26257 --database=tpch5 < %s" % (self.timeout, self.tmpfile)
            self.dsn =  'postgresql://root@localhost:26257/tpch5?sslmode=disable'
            # self.conn = psycopg2.connect(self.dsn, connect_timeout=30)
            # self.conn.autocommit = True
            return
        if (self.scale == "small" and self.dbms == "postgresql"):
            self.run_cmd = "timeout %s psql -p 5432 -t -F ',' --no-align -d tpchsmall -f %s" % (str(self.timeout)+"s", self.tmpfile)
            self.dsn = "dbname=tpchsmall user=" + "root" +" connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
            # self.run_cmd = "timeout %d cockroach sql --insecure --host=localhost --port=26257 --database=tpchsmall < %s" % (self.timeout, self.tmpfile)
            # self.conn = psycopg2.connect(self.dsn, connect_timeout=30)
            # self.conn.autocommit = True
            return
        if (self.scale == "large" and self.dbms == "postgresql"):
            self.run_cmd = "timeout %s psql -p 5432 -t -F ',' --no-align -d tpchsmall -f %s" % (str(self.timeout)+"s", self.tmpfile)
            self.dsn = "dbname=tpchsmall user=" + "root" +" connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
            # self.run_cmd = "timeout %d cockroach sql --insecure --host=localhost --port=26257 --database=tpch5 < %s" % (self.timeout, self.tmpfile)
            # self.conn = psycopg2.connect(self.dsn, connect_timeout=30)
            # self.conn.autocommit = True
            return

    def bootstrap_dbms_benchexp_large(self, dbname):
        if (self.dbms == "postgresql"):
            self.run_cmd = "timeout %s psql -p 5432 -t -F ',' --no-align -d %s -f %s" % (str(self.timeout)+"s", dbname, self.tmpfile)
            self.dsn = "dbname=demo100  user=" + "root" +" connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
        else:
            self.run_cmd = "timeout %d cockroach sql --insecure --host=localhost --port=26257 --database=%s < %s" % (self.timeout, dbname, self.tmpfile)
            self.dsn =  'postgresql://root@localhost:26257/demo100?sslmode=disable'
    # def bootstrap_dbms_benchexp_large(self):
    #     self.run_cmd = "timeout %s psql -p 5432 -t -F ',' --no-align -d demo100 -f %s" % (str(self.timeout)+"s", self.tmpfile)
    #     self.dsn = "dbname=demo100 user=" + "root" +" connect_timeout=30 host=/tmp options='-c statement_timeout=500000'"
    #     return





    def _ret_cursor(self):
        return self.conn.cursor()

    def compare_2_query_output(self, input1, input2):
        # print("begin verify content equivalence")
        with open(self.tmpfile, 'w') as f:
            f.write(input1+";\n")
        cmd = self.run_cmd
        outfile1 = "result1"
        with open(outfile1,"w") as outf1:
            # print(outfile1)
            complete = subprocess.run(cmd, stdout=outf1, stderr=subprocess.PIPE, encoding='utf-8',shell=True)

        with open(self.tmpfile, 'w') as f:
            f.write(input2+";\n")
        cmd = self.run_cmd
        outfile2 = "result2"
        with open(outfile2,"w") as outf2:
            # print(outfile2)
            complete = subprocess.run(cmd, stdout=outf2, stderr=subprocess.PIPE, encoding='utf-8',shell=True)
        arg_str = "sort -o %s %s" % (outfile1, outfile1)
        complete = subprocess.run(arg_str, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',shell=True)
        arg_str_ = "sort -o %s %s" % (outfile2, outfile2)
        complete_ = subprocess.run(arg_str_, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',shell=True)
        output_same = False
        if filecmp.cmp(outfile1,outfile2):
            output_same = True
        os.remove(outfile1)
        os.remove(outfile2)
        return output_same

    def run_postgres_query(self, input):
        error = False
        out = None
        with open(self.tmpfile, 'w') as f:
            f.write("select now();\n")
            f.write(input+";\n")
            f.write("select now();\n")
        cmd = self.run_cmd
        # print("command is", cmd)
        # dsn =  'postgresql://root@localhost:26257/tpch5?sslmode=disable'
        # try:
        #     self.dsn = 'postgresql://root@localhost:26257/tpch1?sslmode=disable'
        #     self.conn = psycopg2.connect(self.dsn, connect_timeout=30)
        # except Exception as inst:
        #     restart_cmd = ["cockroach start-single-node --store=node1 --insecure --listen-addr=localhost:26257 --http-addr=localhost:8080 --background"]
        #     print("try", restart_cmd)
        #     complete = subprocess.run(restart_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout = 5, encoding='utf-8',shell=True)
        #     print("try restart", complete.returncode, file=sys.stderr)

        complete = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',shell=True)
        if complete.returncode != 0:
            pg_process_cmd = 'ps aux | grep "[p]ostgres"'
            complete = subprocess.run(pg_process_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',shell=True)
            if complete.returncode != 0:
                print("postgres process not found", file=sys.stderr)
                print("query is", input, file=sys.stderr)
            # print("fail",complete.returncode, complete.stderr)
            return None, None
        else:
            output_data = complete.stdout
            if ("error" in complete.stderr.lower()):
                # print("error is", complete.stderr)
                return None, None
            lines = output_data.splitlines()
            elapsed, result = calctime(lines, 0, -1, 1, -1)
            # if elapsed == -1:
            #     # TODO -------------------------------------
            #     # Log this kind of bug: timeout?
            #     print(",")
            #     return None, elapsed
            return result, elapsed


    def run_cockroach_query(self, input):
        # set up tmp file for running query
        with open(self.tmpfile, 'w') as f:
            f.write("select now();\n")
            f.write(input+";\n")
            f.write("select now();\n")
        cmd = self.run_cmd
        #print("command is", cmd)

        # dsn =  'postgresql://root@localhost:26257/tpch5?sslmode=disable'
        # try:
        #     self.dsn = 'postgresql://root@localhost:26257/tpch1?sslmode=disable'
        #     self.conn = psycopg2.connect(self.dsn, connect_timeout=30)
        # except Exception as inst:
        #     restart_cmd = ["cockroach start-single-node --store=node1 --insecure --listen-addr=localhost:26257 --http-addr=localhost:8080 --background"]
        #     print("try", restart_cmd)
        #     complete = subprocess.run(restart_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout = 5, encoding='utf-8',shell=True)
        #     print("try restart", complete.returncode, file=sys.stderr)
        # self.conn.close()


        complete = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',shell=True)
        if complete.returncode != 0:
            # print("fail",complete.returncode, complete.stderr)
            # if "internal error" in complete.stderr or "exception" in complete.stderr:
            #     current_time = datetime.now()
            #     # self.validated_perf_query_file.append(filename)
            #     # triage a bug
            #     bug = Bug(ErrorType.OTHER)
            #     found_time = time.time()
            #     bug.set_title("Bug %d internal error %d" % (self.bug_idx, self.idx))
            #     self.bug_idx += 1
            #     bug.set_datetime(str(current_time))
            #     bug.set_rawfile("filename")
            #     bug.set_first_query(input)
            #     bug.set_wrongDB(self.dbms)
            #     bug.set_errormessage(complete.stderr)
            #     with open(self.error_file, 'a+') as f:
            #         f.write(bug.ret_markdown())
            # print(complete.stdout)
            # print(complete.stderr)
            # if complete.returncode != 124:
            #     print(complete.stdout)
            #     print(complete.stderr)
            return None, None
        else:
            output_data = complete.stdout
            if ("error" in complete.stderr.lower()):
                # print("error is", complete.stderr)
                return None, None
            lines = output_data.splitlines()
            elapsed, result = calctime(lines, 1, -1, 2, -2)
            if elapsed == -1:
                # TODO -------------------------------------
                # Log this kind of bug
                return None, None
            return result, elapsed

        # set up timeout
        # and then run the query
        # cur = self._ret_cursor()
        # set_time_out_str = "set statement_timeout=%d;" % self.timeout
        # # print(set_time_out_str)
        # cur.execute(set_time_out_str)
        # start_time = time.time()
        # cur.execute(input)
        # time_result = time.time() - start_time
        # result = cur.fetchall()
        # cur.close()
        # return result, time_result

    def run_queries_time(self, queries, filename):
        # only takes care of 2 kinds of bug: performance diff and estimate inaccurate
        # ignore other error since it has been taken care of in small run
        execution_time_list = []
        executable_query_list = []
        query_list=[]
        # print("run for time",filename)
        failed_count = 0
        found_sig_diff = False
        for i in range(len(queries)-1):
            if ("--" in queries[i]):
                continue
            try:
                query_list.append(queries[i])
                if (self.dbms == "cockroachdb"):
                    _, time_result = self.run_cockroach_query(queries[i] + ";")
                if (self.dbms == "postgresql"):
                    _, time_result = self.run_postgres_query(queries[i] + ";")
                if (time_result is not None):
                    # print("get time result")
                    # if (time_result < 0):
                    #     print("negative time")
                    execution_time_list.append(float(time_result))
                    executable_query_list.append(queries[i])
                else:
                    # print("does not get time result")
                    failed_count += 1
                    execution_time_list.append(float(0))
                if (len(execution_time_list) > failed_count + 1) and (max(execution_time_list) > float(self.diff_threshold) * get_meaninful_min_time(execution_time_list) and get_meaninful_min_time(execution_time_list) > 0.001):
                    found_sig_diff = True
                    break

            except Exception as error:
                pass
                # print ("error but ignored")
        # print(execution_time_list)
        executable_query_list_ = executable_query_list.copy()
        if (found_sig_diff):
            # run content check
            # print("run content check")
            max_time = max(execution_time_list)
            min_time = get_meaninful_min_time(execution_time_list)
            max_idx = execution_time_list.index(max_time)
            min_idx = execution_time_list.index(min_time)
            input1 = query_list[max_idx]
            input2 = query_list[min_idx]
            if (self.compare_2_query_output(input1, input2)) is False:
                return

            # print("serious validation", execution_time_list)
            vote = 0
            for i in range (5):
                _execution_time_list = []
                random.shuffle(executable_query_list)
                for q in executable_query_list:
                    if (self.dbms == "cockroachdb"):
                        _, time_result = self.run_cockroach_query(q + ";")
                    if (self.dbms == "postgresql"):
                        _, time_result = self.run_postgres_query(q + ";")
                    if (time_result is not None):
                        _execution_time_list.append(float(time_result))
                if (len(_execution_time_list) > 0 and max(_execution_time_list) > float(self.diff_threshold) * min(_execution_time_list)):
                    vote += 1
            if vote == 5:
                current_time = datetime.now()
                print("find a performance bug!")
                self.validated_perf_query_file.append(filename)
                # triage a bug
                bug = Bug(ErrorType.PERFORMANCE)
                found_time = time.time()
                bug.set_title("Bug %d perf diff %d" % (self.bug_idx, self.idx))
                self.bug_idx += 1
                bug.set_idx(max_idx, min_idx)
                bug.set_datetime(str(current_time))
                bug.set_rawfile(filename)
                bug.set_first_query(input1)
                bug.set_second_query(input2)
                bug.set_wrongDB(self.dbms)
                bug.set_errormessage("wrong!")
                bug.times = max_time, min_time
                with open(self.error_file, 'a+') as f:
                    f.write(bug.ret_markdown())
            else:
                pass
                # print("time difference not consistent")
        # TODO: estimation error
        # else:
        #     cost_result_list = []
        #     #self.run_queries_cost(executable_query_list_, filename)
        #     if len(cost_result_list) > 0 and (max(cost_result_list) > float(self.diff_threshold) * min(cost_result_list)):
        #     # if there is significant difference in estimated cost but no execution time diff
        #     # we store this query file as interesting and run on large database
        #         print("cost estimation error", cost_result_list)
        #         max_cost = max(cost_result_list)
        #         min_cost = min(cost_result_list)
        #         max_idx = cost_result_list.index(max_cost)
        #         min_idx = cost_result_list.index(min_cost)
        #         # triage a bug
        #         bug = Bug(ErrorType.ESTIMATION)
        #         bug.set_title("Bug %d estimation diff" % self.bug_idx)
        #         self.bug_idx += 1
        #         bug.set_rawfile(filename)
        #         bug.set_first_query(executable_query_list_[max_idx])
        #         bug.set_second_query(executable_query_list_[min_idx])
        #         bug.set_wrongDB(self.dbms)
        #         bug.set_errormessage("wrong!")
        #         bug.estimates = max_cost, min_cost
        #         bug.times = execution_time_list[max_idx], execution_time_list[min_idx]
        #         with open(self.error_file, 'a+') as f:
        #             f.write(bug.ret_markdown())

        return None

    def run_queries_cost(self, queries, filename):
        cost_result_list = []
        print("compare plan cost", filename)
        for query in queries:
            if (self.dbms == "cockroachdb"):
                prefix = "EXPLAIN (OPT, VERBOSE) "
                result,_ = self.run_cockroach_query(prefix + query + ";")
                if (result is not None):
                    estimate_cost = parse_cost_cockroach(result)
                    cost_result_list.append(float(estimate_cost))

            if (self.dbms == "postgresql"):
                prefix = "EXPLAIN "
                result,_ = self.run_postgres_query(prefix + query + ";")
                if (result is not None):
                    estimate_cost = parse_cost_postgres(result)
                    # print(estimate_cost)
                    cost_result_list.append(float(estimate_cost))
        return cost_result_list



    def run_queries(self, queries, filename):
        explain_result_list = []
        executable_query_list=[]
        # stores mapping from query idx to row count
        result_dict={}

        # stores mapping from row count to query idx
        distinct_rows_dict = {}

        # with open(filename) as f:
        #     s = " ".join([x.strip() for x in f])

        #------------------check correctness---------------
        # we assume query pair in calcite is always correct, so we don't check correctness for them
        for i in range(len(queries)-1):
            # calcite start
            # print(i)
            # executable_query_list.append(queries[i])
            # continue
            # calcite end
            if ("select" not in queries[i].lower()):
                continue
            if ("insert" in queries[i] or "INSERT" in queries[i]):
                return
            if ("--" in queries[i]):
                continue
            if (queries[i].lower().endswith("on true") or queries[i].lower().endswith("on false")):
                # this may cause the DBMS to freeze, so we ignore such query
                return
            try:
                result = None
                elapsed = None
                # print("run query", queries[i])
                if (self.dbms == "cockroachdb"):
                    result, elapsed = self.run_cockroach_query(queries[i] + ";")
                elif (self.dbms == "postgresql"):
                    # TODO
                    result, elapsed = self.run_postgres_query(queries[i] + ";")
                # error in running the query
                if result is None and elapsed is None:
                    # print ("result is none and elapsed is none")
                    continue
                # print("elpased is", elapsed)
                executable_query_list.append(queries[i])
                row_count = len(result)
                if row_count not in distinct_rows_dict:
                    distinct_rows_dict[row_count] = i
                else:
                    existing_query = queries[distinct_rows_dict[row_count]]
                    if (len(existing_query)) > len(queries[i]):
                        distinct_rows_dict[row_count] = i

            except psycopg2.OperationalError as error:
                print("timeout")
                return

            except Exception as error:
                pass
                # print ("Oops! An exception occured", error)
                # contain_white_list_error = any(white_list_error in str(error).lower() for white_list_error in white_list_errors)
                # if (contain_white_list_error is False):
                #     # log the exception bug
                #     print("other error",error)
                #     traceback.print_exc()
                #     bug = Bug(ErrorType.OTHER)
                #     bug.set_title("Bug %d" % self.bug_idx)
                #     self.bug_idx += 1
                #     bug.set_rawfile(filename)
                #     bug.set_first_query(queries[i])
                #     bug.set_wrongDB(self.dbms)
                #     bug.set_errormessage(str(error) + str(type(error)))
                #     with open(self.error_file, 'a+') as f:
                #         f.write(bug.ret_markdown())
                # else:
                #     pass
                    # print("white list error" + str(type(error)))


        # calcite start
        # cost_result_list = self.run_queries_cost(executable_query_list, filename)
        # print(cost_result_list)
        # if len(cost_result_list) > 0 and (max(cost_result_list) > 1.1 * min(cost_result_list)):
        # # if there is difference in generated plan (approximated through execution cost)
        # # we store this query file as interesting and run on large database
        #     print("found plan diff", filename)
        #     self.perf_query_file.append(filename)
        # calcite end
        # store executable query file for feedback loop
        if len(executable_query_list) > 1:
            self.executable_query_file.append(filename)
            # print("This query file is executable")


        # if len(distinct_rows_dict) > 1:
        #     # there are at least two different row counts:
        #     bug = Bug(ErrorType.CORRECTNESS)
        #     bug.set_title("Bug %d correcteness" % self.bug_idx)
        #     self.bug_idx += 1
        #     bug.set_rawfile(filename)
        #     bug.set_wrongDB(self.dbms)
        #     rows = []
        #     query_idx = []
        #     for k,v in distinct_rows_dict.items():
        #         rows.append(k)
        #         query_idx.append(v)
        #     first_query = queries[query_idx[0]]
        #     second_query = queries[query_idx[1]]


        #     bug.set_first_query(first_query)
        #     bug.set_second_query(second_query)
        #     bug.set_errormessage("output wrong!")
        #     bug.rows = rows[0], rows[1]
        #     with open(self.error_file, 'a+') as f:
        #             f.write(bug.ret_markdown())

        # row count are the same, now check the plan
        if len(distinct_rows_dict) == 1 and len(executable_query_list) > 1:
        #------------------check estimate cost---------------
            # print(distinct_rows_dict, executable_query_list)
            cost_result_list = self.run_queries_cost(executable_query_list, filename)
            # print(cost_result_list)
            if len(cost_result_list) > 0 and (max(cost_result_list) !=  min(cost_result_list)):
            # if: (1) there is difference in generated plan (approximated through execution cost)
            #     (2) query returns non-empty result
            # we store this query file as interesting and run on large database
                if (list(distinct_rows_dict.keys())[0] > 1):
                    print("find plan diff", filename)
                    self.perf_query_file.append(filename)
        else:
            if (len(distinct_rows_dict) > 1):
                # print("fail plan cost info due to correctness", distinct_rows_dict,filename)
                pass
            else:
                # print("fail plan cost info due to executable query", filename)
                pass



# e = Executor("cockroach", "small")
# e.bootstrap_dbms()
# query = "select 1 from nation;"
# e_query = "EXPLAIN (OPT, VERBOSE)" + query
# result = e.run_cockroach_query(e_query)
# print(parse_cost_cockroach(result))
# print(result)
