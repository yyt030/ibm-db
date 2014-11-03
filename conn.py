#!C:\Python27\python.exe
# -*- coding: utf-8 -*-
# Author: Yantao

import sys 
import ibm_db as db2
import datetime
import time


def connDb(dbname, dbuser, dbpasswd, host='182.29.0.1', port=51001, autocomm=1):
    conn = db2.connect("HOSTNAME=%s;PROTOCOL=TCPIP;\
        PORT=%d;DATABASE=%s;UID=%s;PWD=%s;" \
                       %(host, port, dbname, dbuser, dbpasswd), '', '')
    if autocomm == 0:
        db2.autocommit(conn, db2.SQL_AUTOCOMMIT_OFF)
    return conn

def pconnDb(dbname, dbuser, dbpasswd, host='182.29.0.1', port=51001):
    array = { db2.SQL_ATTR_AUTOCOMMIT : db2.SQL_AUTOCOMMIT_OFF }
    conn = db2.pconnect("HOSTNAME=%s;PROTOCOL=TCPIP;\
        PORT=%d;DATABASE=%s;UID=%s;PWD=%s;" \
                       %(host, port, dbname, dbuser, dbpasswd), '', '',array)
    return conn

def getResByDict(conn, sql):
    ''' Call fetch_both: '''
    print sql
    stmt = db2.exec_immediate(conn, sql)
    res = db2.fetch_both(stmt)
    while(res):
        print 'Result from :', res[1], res
        res = db2.fetch_both(stmt)
        
def getResByTuple(conn, sql, commitnum = 500):
    ''' Call fetch_tuple: '''
    num = 0
    tbname = 'TEST'
    stmt = db2.exec_immediate(conn, sql)
    res = db2.fetch_tuple(stmt)
    
    list1 = []
    while res != False:
        insql = "insert into %s values(?,?,?,?,?,?,\
                ?,?,?,?,?,?,?,?,?,?,\
                ?,?,?,?,?,?,?,?,?,?,?,\
                ?, ?, ?, ?)" %(tbname)
        list1.append(res)
        res = db2.fetch_tuple(stmt)
        num += 1
        if len(list1) == commitnum or res == False:
            stmt2 = db2.prepare(conn, insql)
            db2.execute_many(stmt2, tuple(list1)) 
            list1 = []
            print "insert into records[%d]... " %num
    return num

def getBatResByTuple(conn, sql, commitnum=500):
    ''' Call fetch_tuple: '''
    num = 0
    tbname = 'TEST'
    stmt = db2.exec_immediate(conn, sql)
    res = db2.fetch_tuple(stmt)
    while res != False:
        insql = "insert into %s values('%s','%s','%s','%s','%s','%s',\
                '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',\
                '%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,'%s',\
                %s, %s, %s, %s)" \
                %(tbname, res[0],res[1],res[2],res[3],res[4],res[5],\
                  res[6],res[7],res[8],res[9],res[10],res[11],\
                  res[12],res[13],res[14],\
                  res[15],res[16],res[17],res[18],res[19],res[20],\
                  res[21] or 'Null',\
                  res[22] or 'Null',res[23] or 'Null',\
                  res[24] or 'Null',res[25] or 'Null',res[26] or 'Null', \
                  res[27] or 'Null',res[28] or 'Null',res[29] or 'Null',\
                  res[30] or 'Null')
        ret = insertRec(conn, insql)
        if ret == True:
            num += 1
        if num % commitnum == 0:
            print 'current records:%d, transcation commit.' %num
            db2.commit(conn)
            
        res = db2.fetch_tuple(stmt)
    db2.commit(conn)
    return num

def getBatRes(conn, sql, commitnum=500):
    ''' Call fetch_tuple: '''
    num = 0
    tbname = 'TEST'
    stmt = db2.exec_immediate(conn, sql)
    res = db2.fetch_tuple(stmt)
    while res != False:
        insql = "insert into %s values('%s','%s','%s','%s','%s','%s',\
                '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',\
                '%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,'%s',\
                %s, %s, %s, %s)" \
                %(tbname, res[0],res[1],res[2],res[3],res[4],res[5],\
                  res[6],res[7],res[8],res[9],res[10],res[11],\
                  res[12],res[13],res[14],\
                  res[15],res[16],res[17],res[18],res[19],res[20],\
                  res[21] or 'Null',\
                  res[22] or 'Null',res[23] or 'Null',\
                  res[24] or 'Null',res[25] or 'Null',res[26] or 'Null', \
                  res[27] or 'Null',res[28] or 'Null',res[29] or 'Null',\
                  res[30] or 'Null')
        ret = insertRec(conn, insql)
        if ret == True:
            num += 1
        if num % commitnum == 0:
            print 'current records:%d, transcation commit.' %num
            db2.commit(conn)
            
        res = db2.fetch_tuple(stmt)
    db2.commit(conn)
    return num
        
def getResByRow(conn, sql):
    ''' Call fetch_row '''
    n = 0
    try:
        stmt = db2.exec_immediate(conn, sql)
        res = db2.fetch_row(stmt)
        while res != False:
            print 'Result from :', db2.result(stmt, 0)
            n += db2.result(stmt, 0)
            res = db2.fetch_row(stmt)
    except:
        print "Transaction couldn't be completed:" , db2.stmt_errormsg()
    else:
        return n
    
def insertRec(conn, sql):
    ''' Call num_rows '''
    try:
        stmt = db2.exec_immediate(conn, sql) 
        #print 'Number of affected rows:', db2.num_rows(stmt)
    except:
        print sql
        print "Transaction couldn't be completed:" , db2.stmt_errormsg()
        return False
    else:
        return True

if __name__ == '__main__':
    conn = connDb('TEST','TEST','TEST', autocomm=1)
    # insert records
    time.sleep(5)
    fetchnum = 50000 
    sql = "select * from TEST \
           where RESPONSEFLAG = '1' fetch first %d rows only" \
          %fetchnum
    starttime = datetime.datetime.now()
    n = getResByTuple(conn, sql, 1000)
    endtime = datetime.datetime.now()
    print 'All cost time, All TPS, Success TPS: %s %f %f' \
          %(endtime - starttime, \
          fetchnum*(10.0**6)/((endtime - starttime).seconds*(10**6) +  \
          (endtime - starttime).microseconds),\
            n*(10.0**6)/((endtime - starttime).seconds*(10**6) +  \
          (endtime - starttime).microseconds))
    # check records
    print 'start check result ...'
    f = file('check_res.sql')
    checksql = f.read()
    time.sleep(10)
    #res = getResByRow(conn, checksql)
    res = 0
    if res == 0:
        print 'the result of test is ok'
    else:
        print 'the result of test is FAILED!'
        print res
    db2.close(conn)
    f.close()
    print '----- TEST END -----'

