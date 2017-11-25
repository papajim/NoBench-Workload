import random
import json

class QueryHandler:
    def q1(self):
        query = "SELECT JSON_VALUE(NOBENCH_DOC, '$.str1') AS str1, JSON_VALUE(NOBENCH_DOC, '$.num') AS num FROM :table"
        print query

    def q2(self):
        query = "SELECT JSON_VALUE(NOBENCH_DOC, '$.nested_obj.str') AS str, JSON_VALUE(NOBENCH_DOC, '$.nested_obj.num') AS num FROM :table"

    def q3(self):
        query = "SELECT JSON_VALUE(NOBENCH_DOC, :sparse_xx0) AS :sparse_xx0_name, JSON_VALUE(NOBENCH_DOC, :sparse_xx9) as :sparse_xx9_name FROM :table"
        numx = random.randint(0, 99)
        snumx = str(numx).zfill(2)
        sparse_xx0 = "'$.sparse_{0}0'".format(snumx)
        sparse_xx0_name = "sparse_{0}0".format(snumx)
        sparse_xx9 = "'$.sparse_{0}9'".format(snumx)
        sparse_xx9_name = "sparse_{0}9".format(snumx)

    def q4(self):
        query = "SELECT JSON_VALUE(NOBENCH_DOC, :sparse_xx0) AS :sparse_xx0_name, JSON_VALUE(NOBENCH_DOC, :sparse_yy0) as :sparse_yy0_name FROM :table"
        numx = random.randint(0, 99)
        numy = random.randint(0, 99)
        snumx = str(numx).zfill(2)
        snumy = str(numy).zfill(2)
        sparse_xx0 = "'$.sparse_{0}0'".format(snumx)
        sparse_xx0_name = "sparse_{0}0".format(snumx)
        sparse_yy0 = "'$.sparse_{0}9'".format(snumy)
        sparse_yy0_name = "sparse_{0}9".format(snumy)

    def q5(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, '$.str1') = :str1"
        str1_index = random.randrange(0, self.str1_options_len)
        print self.str1_options[str1_index]

    def q6(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, '$.num') BETWEEN :start AND :end"
        startNum = random.randint(self.range[0], self.range[1])
        endNum = random.randint(self.range[0], self.range[1])

    def q7(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, '$.dyn1') BETWEEN :start AND :end"
        startNum = random.randint(self.range[0], self.range[1])
        endNum = random.randint(self.range[0], self.range[1])

    def q8(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$.nested_arr') AS nested_arr FROM :table WHERE JSON_EXISTS(NOBENCH_DOC,'$?(@.nested_arr == $ITEM)' PASSING :item AS 'ITEM')"
        nested_index = random.randrange(0, self.nested_options_len)
        print self.nested_options[nested_index]

    def q9(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, :sparse_xxx) = :value"
        num = random.randint(0, 999)
        snum = str(numx).zfill(3)
        sparse_xxx = "'$.sparse_{0}'".format(snum)
        sparse_value_index = random.randrange(0, self.sparse_options_len)
        print self.sparse_options[sparse_value_index]

    def q10(self):
        query = "SELECT count(*) FROM :table WHERE (JSON_VALUE(NOBENCH_DOC, '$.num') BETWEEN :start AND :end) GROUP BY JSON_VALUE(NOBENCH_DOC, '$.thousandth')"
        startNum = random.randint(self.range[0], self.range[1])
        endNum = random.randint(self.range[0], self.range[1])

    def q11(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table left INNER JOIN :table right ON JSON_VALUE(left.NOBENCH_DOC, '$.str1') = JSON_VALUE(right.NOBENCH_DOC, '$.nested_obj.str') WHERE JSON_VALUE(left.NOBENCH_DOC, '$.num') BETWEEN :start AND :end"
        startNum = random.randint(self.range[0], self.range[1])
        endNum = random.randint(self.range[0], self.range[1])

################################################
#### Controls
################################################

    handlers = {1: q1, 2: q2, 3: q3, 4: q4, 5: q5, 6: q6, 7: q7, 8: q8, 9: q9, 10: q10, 11: q11}
    
    def __init__(self, queryType, table, num_range, lookup_files):
        self.queryType = queryType
        self.table = table
        self.range = num_range
        self.str1_options = lookup_files["str1_options"]
        self.str1_options_len = lookup_files["str1_options_len"]
        self.dyn1_options = lookup_files["dyn1_options"]
        self.dyn1_options_len = lookup_files["dyn1_options_len"]
        self.nested_options = lookup_files["nested_options"]
        self.nested_options_len = lookup_files["nested_options_len"]
        self.sparse_options = lookup_files["sparse_options"]
        self.sparse_options_len = lookup_files["sparse_options_len"]
        self.handler = self.handlers[queryType]


    def run(self):
        self.handler(self)

