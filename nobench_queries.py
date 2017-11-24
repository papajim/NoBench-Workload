import random

class QueryHandler:
    def __init__(self, queryType, table):
        self.queryType = queryType
        self.table = table

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
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, '$.str1') = :str1",

    def q6(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, '$.num') BETWEEN :start AND :end"

    def q7(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, '$.dyn1') BETWEEN :start AND :end"

    def q8(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$.nested_arr') AS nested_arr FROM :table WHERE JSON_EXISTS(NOBENCH_DOC,'$?(@.nested_arr == $ITEM)' PASSING :item AS 'ITEM')",

    def q9(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table WHERE JSON_VALUE(NOBENCH_DOC, :sparse_xxx) = :value",
        num = random.randint(0, 999)
        snum = str(numx).zfill(3)
        sparse_xxx = "'$.sparse_{0}'".format(snum)

    def q10(self):
        query = "SELECT count(*) FROM :table WHERE (JSON_VALUE(NOBENCH_DOC, '$.num') BETWEEN :start AND :end) GROUP BY JSON_VALUE(NOBENCH_DOC, '$.thousandth')"

    def q11(self):
        query = "SELECT JSON_QUERY(NOBENCH_DOC, '$') AS nobench_doc FROM :table left INNER JOIN :table right ON JSON_VALUE(left.NOBENCH_DOC, '$.str1') = JSON_VALUE(right.NOBENCH_DOC, '$.nested_obj.str') WHERE JSON_VALUE(left.NOBENCH_DOC, '$.num') BETWEEN :start AND :end"
