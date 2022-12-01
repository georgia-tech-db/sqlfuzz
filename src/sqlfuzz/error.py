from enum import Enum
import sqlparse
import datetime
class ErrorType(Enum):
    SYNTAX = 1
    OTHER = 2
    CORRECTNESS = 3
    PERFORMANCE = 4
    ESTIMATION = 5
class Error(object):
    def __init__(self, errortype, queryname):
        self.query_name = queryname
        # self.query_list = []
        self.error_type = errortype
    # # query_name should be a tuple encoding (file_name, idx in the file)
    # def update_firing_query(self, query_name):
    #     self.query_list.append(query_name)
    #     self.frequency += 1
    def output(self):
        print("---print summary of this error---")
        print(self.error_type)
        print(self.query_name)

class Bug(object):
    def __init__(self, errortype):
        self.first_query = None
        self.second_query = None

        self.title = None
        # self.size = None
        self.rows = None
        self.times = None
        self.estimates = None

        self.rawinput = None
        self.wrongdb = None
        self.type = errortype
        self.error_message = None
        self.time_str = None
        self.first_idx = None
        self.second_idx = None
    def set_idx(self, first, second):
        self.first_idx = first
        self.second_idx = second
    def set_datetime(self, dt_string):
        self.time_str = dt_string

    def set_first_query(self, query):
        self.first_query = sqlparse.format(query, reindent=True)
    
    def set_second_query(self, query):
        self.second_query = sqlparse.format(query, reindent=True)

    def set_title(self, title):
        self.title = title

    def set_rawfile(self, _in):
        self.rawinput = _in

    def set_errormessage(self, message):
        self.error_message = message

    def set_wrongDB(self, db):
        self.wrongdb = db

    def ret_markdown(self):
        if self.type == None:
            return None
        if self.type == ErrorType.CORRECTNESS:
            template = """
<details><summary>{1}</summary>
<p>

#### General information
* raw file: {in}
* different DB: {db}
* error type: correctness bug

#### Query
```sql
{2}
```
```sql
{3}
```

#### Summary
|           | First Query | Second Query |
|-----------|-------------|--------------|
| # of rows | {4} |
</p>
</details>
"""
            
        if self.type == ErrorType.PERFORMANCE:
            template = """
<details><summary>{1}</summary>
<p>

#### General information
* raw file: {in}
* generatetime: {time}
* different DB: {db}
* error type: performance bug

#### Query
```sql
{2}
```
```sql
{3}
```

#### Summary
|           | First Query | Second Query |
|-----------|-------------|--------------|
| time      | {5} |
| idx      | {10} |

</p>
</details>
"""
        if self.type == ErrorType.ESTIMATION:
            template = """
<details><summary>{1}</summary>
<p>

#### General information
* raw file: {in}
* different DB: {db}
* error type: performance bug

#### Query
```sql
{2}
```
```sql
{3}
```

#### Summary
|           | First Query | Second Query |
|-----------|-------------|--------------|
| time      | {5} |
| estimate  | {7} |

</p>
</details>
"""
        if self.type == ErrorType.OTHER:
            template = """
<details><summary>{1}</summary>
<p>

#### General information
* raw file: {in}
* different DB: {db}
* error type: unexpected error

#### Query
```sql
{2}
```

#### Error info:
{6}
</p>
</details>
"""
        if (self.type == ErrorType.OTHER):
            template = template.replace("{1}", self.title)
        else:
            template = template.replace("{1}", self.title + str(self.type))
        template = template.replace("{in}", self.rawinput)
        template = template.replace("{db}", self.wrongdb)

        if (self.type == ErrorType.CORRECTNESS or self.type == ErrorType.PERFORMANCE or self.type == ErrorType.ESTIMATION):
            template = template.replace("{2}", self.first_query)
            template = template.replace("{3}", self.second_query)
        if (self.type == ErrorType.OTHER):
            template = template.replace("{2}", self.first_query)
            template = template.replace("{6}", self.error_message)


        if (self.type == ErrorType.CORRECTNESS):    
            first_rows, second_rows = self.rows
            row_str = " %d | %d  " % (first_rows, second_rows)
            template = template.replace("{4}", row_str)

        if (self.type == ErrorType.PERFORMANCE):    
            fast_time, slow_time = self.times
            max_idx = self.first_idx
            min_idx = self.second_idx
            time_str = " %f | %f  " % (fast_time, slow_time)
            idx_str = " %d | %d  " % (max_idx, min_idx)

            template = template.replace("{5}", time_str)
            template = template.replace("{time}", self.time_str)
            template = template.replace("{10}", idx_str)


        
        if (self.type == ErrorType.ESTIMATION):    
            fast_estimate, slow_estimate = self.estimates
            fast_time, slow_time = self.times
            time_str = " %f | %f  " % (fast_time, slow_time)
            est_str = " %f | %f  " % (fast_estimate, slow_estimate)
            template = template.replace("{7}", est_str)
            template = template.replace("{5}", time_str)
            print(fast_time, slow_time)
            print(fast_estimate, slow_estimate)


        



        return template



# --------------Unit test------------
# ---other---:
# bug = Bug(ErrorType.OTHER)
# bug.set_title("bug1")
# bug.set_rawfile("demo.sql")
# bug.set_first_query("select 1 from nation on true;")
# bug.set_wrongDB("cockroach")
# bug.set_errormessage("wrong!")

# ---correctness---:
# bug = Bug(ErrorType.CORRECTNESS)
# bug.set_title("bug1")
# bug.set_rawfile("demo.sql")
# bug.set_first_query("select 1 from nation on true;")
# bug.set_second_query("select 1 from nation on true;")
# bug.set_wrongDB("cockroach")
# bug.rows = 2,3

# # ---performance---:
# bug = Bug(ErrorType.PERFORMANCE)
# bug.set_title("bug1")
# bug.set_rawfile("demo.sql")
# bug.set_first_query("select 1 from nation on true;")
# bug.set_second_query("select 1 from nation on true;")
# bug.set_wrongDB("cockroach")
# bug.set_errormessage("wrong!")
# bug.times = 2,3




# output = bug.ret_markdown()
# with open("demo.md", 'w') as f:
#     f.write(output)