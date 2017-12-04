import random
import json

class QueryHandler:
    def q1(self):
        query = "SELECT JSON_VALUE(NOBENCH_DOC, '$.str1') AS str1, JSON_VALUE(NOBENCH_DOC, '$.num') AS num FROM {0}".format(self.table)
        self.cur.execute(query)

    def q2(self):
	query = "SELECT JSON_VALUE(NOBENCH_DOC, '$.nested_obj.str') AS str, JSON_VALUE(NOBENCH_DOC, '$.nested_obj.num') AS num FROM {0}".format(self.table)
	self.cur.execute(query)

    def q3(self):
        numx = random.randint(0, 99)
        snumx = str(numx).zfill(2)
        sparse_xx0 = "'$.sparse_{0}0'".format(snumx)
        sparse_xx0_name = "sparse_{0}0".format(snumx)
        sparse_xx9 = "'$.sparse_{0}9'".format(snumx)
        sparse_xx9_name = "sparse_{0}9".format(snumx)
        query = "SELECT JSON_VALUE(NOBENCH_DOC, {0}) AS {1}, JSON_VALUE(NOBENCH_DOC, {2}) as {3} FROM {4}".format(sparse_xx0, sparse_xx0_name, sparse_xx9, sparse_xx9_name, self.table)
        self.cur.execute(query)

    def q4(self):
        numx = random.randint(0, 99)
        numy = random.randint(0, 99)
        snumx = str(numx).zfill(2)
        snumy = str(numy).zfill(2)
        sparse_xx0 = "'$.sparse_{0}0'".format(snumx)
        sparse_xx0_name = "sparse_{0}0".format(snumx)
        sparse_yy0 = "'$.sparse_{0}9'".format(snumy)
        sparse_yy0_name = "sparse_{0}9".format(snumy)
        query = "SELECT JSON_VALUE(NOBENCH_DOC, {0}) AS {1}, JSON_VALUE(NOBENCH_DOC, {2}) as {3} FROM {4}".format(sparse_xx0, sparse_xx0_name, sparse_yy0, sparse_yy0_name, self.table)
        self.cur.execute(query)

    def q5(self):
        index = random.randint(self.range[0], self.range[1])
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM {0} WHERE JSON_VALUE(NOBENCH_DOC, '$.str1') = '{1}'".format(self.table, self.lookup_values[index]["str1"])
        self.cur.execute(query)

    def q6(self):
        startNum = random.randint(self.range[0], self.range[1]-1)
        endNum = random.randint(startNum, self.range[1])
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM {0} WHERE JSON_VALUE(NOBENCH_DOC, '$.num') BETWEEN {1} AND {2}".format(self.table, startNum, endNum)
        self.cur.execute(query)

    def q7(self):
        startNum = random.randint(self.range[0], self.range[1]-1)
        endNum = random.randint(startNum, self.range[1])
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM {0} WHERE JSON_VALUE(NOBENCH_DOC, '$.dyn1' RETURNING NUMBER) BETWEEN {1} AND {2}".format(self.table, startNum, endNum)
        self.cur.execute(query)

    def q8(self):
        nested_index = random.randrange(0, self.lookup_values_text["nested_options_len"])
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$.nested_arr') AS nested_arr FROM {0} WHERE JSON_EXISTS(NOBENCH_DOC,'$?(@.nested_arr == $ITEM)' PASSING '{1}' AS 'ITEM')".format(self.table, self.lookup_values_text["nested_options"][nested_index])
        print query
        self.cur.execute(query)

    def q9(self):
        num = random.randint(0, 999)
        snum = str(num).zfill(3)
        sparse_xxx = "'$.sparse_{0}'".format(snum)
        sparse_value_index = random.randrange(0, self.lookup_values_text["sparse_options_len"])
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM {0} WHERE JSON_VALUE(NOBENCH_DOC, {1}) = '{2}'".format(self.table, sparse_xxx, self.lookup_values_text["sparse_options"][sparse_value_index])
        self.cur.execute(query)

    def q10(self):
        startNum = random.randint(self.range[0], self.range[1])
        endNum = random.randint(self.range[0], self.range[1])
        query = "SELECT count(*) FROM {0} WHERE (JSON_VALUE(NOBENCH_DOC, '$.num') BETWEEN {1} AND {2}) GROUP BY JSON_VALUE(NOBENCH_DOC, '$.thousandth')".format(self.table, startNum, endNum)
        self.cur.execute(query)

    def q11(self):
        startNum = random.randint(self.range[0], self.range[1])
        endNum = random.randint(self.range[0], self.range[1])
        query = "SELECT JSON_QUERY(left.NOBENCH_DOC, '$') AS nobench_doc FROM {0} left INNER JOIN {1} right ON JSON_VALUE(left.NOBENCH_DOC, '$.str1') = JSON_VALUE(right.NOBENCH_DOC, '$.nested_obj.str') WHERE JSON_VALUE(left.NOBENCH_DOC, '$.num') BETWEEN {2} AND {3}".format(self.table, self.table, startNum, endNum)
        self.cur.execute(query)

    def q12(self):
        num = random.randint(self.range[0], self.range[1])
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM {0} WHERE nobench_num = {1}".format(self.table, num)
        self.cur.execute(query)
                                


################################################
#### Controls
################################################

    handlers = {1: q1, 2: q2, 3: q3, 4: q4, 5: q5, 6: q6, 7: q7, 8: q8, 9: q9, 10: q10, 11: q11, 12: q12}
    
    def __init__(self, cursor, queryType, table, num_range, lookup_values, lookup_values_text):
        self.cur = cursor
        self.queryType = queryType
        self.table = table
        self.range = num_range
        self.lookup_values = lookup_values
        self.lookup_values_text = lookup_values_text
        self.handler = self.handlers[queryType]


    def run(self):
        self.handler(self)

