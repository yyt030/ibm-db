#!C:\Python27\python.exe
# coding: utf8


__author__ = 'yueyt'

import time
import ibm_db
import MySQLdb


class OperDb2:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

        self.manual_commit = True

    def get_conn(self):
        conn = ibm_db.connect("HOSTNAME=%s;PROTOCOL=TCPIP;PORT=%d;DATABASE=%s;UID=%s;PWD=%s;"
                              % (self.host, self.port, self.dbname, self.user, self.password), '', '')
        # turn off autocommit: SQL_AUTOCOMMIT_OFF
        if self.manual_commit:
            ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
        return conn

    def get_source_data(self, querysql, queue):
        """ Call fetch_row """
        num = 0
        conn = self.get_conn()
        try:
            stmt = ibm_db.exec_immediate(conn, querysql)
            result = ibm_db.fetch_tuple(stmt)
            while result:
                queue.put(result)
                num += 1
                result = ibm_db.fetch_tuple(stmt)
        except Exception as e:
            print e.args
        return num

    def insert_record(self, tablename, record):
        conn = self.get_conn()
        insertsql = "insert into %s(...) values(?,?,?,?,?,?,\
                ?,?,?,?,?,?,?,?,?,?,\
                ?,?,?,?,?,?,?,?,?,?,?,\
                ?, ?, ?, ?)" % tablename
        stmt = ibm_db.prepare(conn, insertsql)
        ibm_db.execute(stmt, record)
        ibm_db.commit(conn)


    def insert_many_records(self, recordslist, commitnum, tablename=None):
        conn = self.get_conn()
        for i in xrange(0, len(recordslist), commitnum):
            time.sleep(0.05)
            if tablename is None:
                import random

                tbname = 'EGSPM_TRANSACTION_TRACE_%d' % (random.randint(1, 5))
            else:
                tbname = tablename
            insertsql = "insert into %s(...) values(?, ?, ?, ?, ?, ?, \
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                    ?, ?, ?, ?)" % tbname
            try:
                stmt = ibm_db.prepare(conn, insertsql)
                ibm_db.execute_many(stmt, tuple(recordslist[i:i + commitnum]))
            except:
                print ibm_db.stmt_error(), ibm_db.stmt_errormsg()
            finally:
                ibm_db.commit(conn)


    def close_conn(self):
        conn = self.get_conn()
        ibm_db.close(conn)


class OperMysql:
    def __init__(self, host, port, dbname, user, passwd):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.passwd = passwd

        self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=dbname, charset='utf8',
                                  use_unicode=True)
        self.cursor = self.db.cursor()
        # self.db.autocommit(on=True)

    def insert_many_records(self, recordslist, commitnum, tablename=None):
        for i in xrange(0, len(recordslist), commitnum):
            if tablename is None:
                import random

                tbname = 'EGSPM_TRANSACTION_TRACE_%d' % (random.randint(1, 5))
            else:
                tbname = tablename
            insertsql = "insert into " + tbname + "(...) values(%s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s)"
            try:
                self.cursor.executemany(insertsql, tuple(recordslist[i:i + commitnum]))
            except self.db.Error, e:
                print 'Error:{}'.format(e)
            finally:
                self.db.commit()

    def insert_record(self, tablename, record):
        insertsql = "insert into " + tablename + "(...) values(%s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s)"
        self.db.execute(insertsql, record)
        self.db.commit()

    def del_records(self, tablename, commitnum):
        fetch_sql = "select mqid from " + tablename
        del_sql = "delete from " + tablename + " where MQID = %s"
        self.cursor.execute(fetch_sql)
        mqid = self.cursor.fetchmany(10000)
        n = 0
        for r in mqid:
            self.cursor.execute(del_sql, r)
            n += 1
            if n >= commitnum:
                self.db.commit()
            n = 0
        self.db.commit()


if __name__ == '__main__': 
    mydb2 = OperDb2('xxx', 52000, 'newdb2', 'xxx', 'xxx')
    mysql = OperMysql('xxx', 3306, 'test', 'xxx', 'xxx')

    with file(r'getRec.sql') as f:
        query_sql = f.read() % (1000 * 1)
        query_datalist = mydb2.get_source_data(query_sql)
        import datetime

        start_time = time.time()
        print 'current time:{}, start_time:{}'.format(datetime.datetime.now(), start_time)

        mysql.insert_many_records(query_datalist, 2000, 'EGSPM_TRANSACTION_TRACE')
        end_time = time.time()
        print 'current time:{}, end time:{}, taken:{}'.format(datetime.datetime.now(),
                                                              end_time, end_time - start_time)
