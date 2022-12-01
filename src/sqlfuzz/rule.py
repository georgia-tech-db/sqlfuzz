class Rule(object):
    def __init__(self, rulename):
        self.rule_name = rulename
        self.query_list = []
        self.frequency = 0
    # query_name should be a tuple encoding (file_name, idx in the file)
    def update_firing_query(self, query_name):
        self.query_list.append(query_name)
        self.frequency += 1
def dummy_function():
    print("hello world")