import pymysql.cursors


class MysqlCursor():
    def __init__(self, host_, db_name_, user_name, password):
        self.host_ip = host_
        self.dataBase = db_name_
        self.userName = user_name
        self.password = password
        self.conn = self.get_mysql_conn()

    def get_mysql_conn(self):
        connection = pymysql.connect(host=self.host_ip,
                                     user=self.userName,
                                     db=self.dataBase,
                                     password=self.password,
                                     autocommit=True,
                                     cursorclass=pymysql.cursors.DictCursor,
                                     charset='utf8',
                                     max_allowed_packet=16 * 1024)
        return connection

    def get_last_id(self, ms_connection):
        with ms_connection.cursor() as ms_cursor:
            ins_id_query = 'SELECT LAST_INSERT_ID();'
            ins_id = self.mysql_action(ms_connection, ins_id_query, 'select')
        ms_cursor.close()
        return ins_id

    def get_affected_rows(self, ms_connection):
        with ms_connection.cursor() as ms_cursor:
            ins_id_query = 'SELECT ROW_COUNT();'
            ins_id = self.mysql_action(ms_connection, ins_id_query, 'select')
        ms_cursor.close()
        return ins_id

    def mysql_action(self, connection, query, q_type):
        print(query)
        if "'now()'" in str(query):
            query = query.replace("'now()'", 'now()')
        with connection.cursor() as cursor:
            cursor.execute(query)
            if q_type == 'select':
                result = cursor.fetchall()
            elif q_type == 'insert':
                result = self.get_last_id(connection)[0]['LAST_INSERT_ID()']
            elif q_type == 'update':
                result = str(self.get_affected_rows(connection)[0]['ROW_COUNT()']) + 'row(s) affected '
            else:
                result = None
        cursor.close()
        return result

    def check_table_existenxce_(self, table):
        table_check_query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'base table' " \
                            "AND table_schema='%s'" % self.dataBase
        table_check = self.mysql_action(self.conn, table_check_query, 'select')
        table_check = [list(i.values())[0] for i in table_check]
        table_check = True if table in table_check else False
        if not table_check:
            raise ValueError('Entered table --%s-- is not in the given schema. Check the table name!' % table)
        else:
            return True

    def insert_values(self, table, row_values):
        table_validation = self.check_table_existenxce_(table)
        if table_validation:
            columns = ",".join([self.conn.escape_string(str(i)) for i in row_values.keys()])
            records = "','".join([self.conn.escape_string(str(i)) for i in row_values.values()])
            query = "insert into %s (%s) values ('%s')" % (table, columns, records)
            return self.mysql_action(self.conn, query, 'insert')

    def update_table(self, table, row_values, condition):
        table_validation = self.check_table_existenxce_(table)
        if table_validation:
            columns = ",".join(
                [self.conn.escape_string(key) + "= '" + self.conn.escape_string(str(row_values[key])) + "'" for key in
                 row_values.keys()])
            condition = " and ".join(
                [self.conn.escape_string(key) + "= '" + self.conn.escape_string(str(condition[key])) + "'" for key in
                 condition.keys()])
            query = "update %s set %s where %s" % (table, columns, condition)
            return self.mysql_action(self.conn, query, 'update')

    def select_rows(self, table, row_values, primary_key):
        table_validation = self.check_table_existenxce_(table)
        if table_validation:
            condition = " and ".join(
                [self.conn.escape_string(key) + "= '" + self.conn.escape_string(str(row_values[key])) + "'" for key in
                 row_values.keys()])
            query = "select * from %s where %s order by %s desc" % (table, condition, primary_key)
            return self.mysql_action(self.conn, query, 'select')