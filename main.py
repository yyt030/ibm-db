#!C:\Python27\python.exe
# coding: utf8

__author__ = 'yueyt'

import Queue
import threading
import time

from utils import OperDb2
from utils import OperMysql

log_queue = Queue.Queue()


class ConsumerRecords(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

        # test mysql
        self.db = OperMysql('xxx', 3306, 'test', 'root', 'root')
        self.tabname = 'xxx'
        self.commitnum = 2000

    def run(self):
        print '{} starting ...'.format(self.name)
        start_time = time.time()
        recordslist = []
        record = []
        num = 0
        while record is not 'quit':
            try:
                record = self.queue.get_nowait()
                if record is not 'quit' and record is not None:
                    recordslist.append(record)

                if len(recordslist) >= self.commitnum:
                    self.db.insert_many_records(recordslist, self.commitnum, self.tabname)
                    num += len(recordslist)
                    recordslist = []

            except Queue.Empty:
                pass
        if len(recordslist):
            self.db.insert_many_records(recordslist, self.commitnum, self.tabname)
            num += len(recordslist)
        log_queue.put([self.name, num, '{0:.6f}'.format(time.time() - start_time),
                       '{0:.6f}'.format(num / (time.time() - start_time))])

        # self.db.close_conn()
        print '{} ending ...'.format(self.name)

    def run1(self):
        while not self.queue.empty():
            record = self.queue.get()
            self.db.insert_record(self.tabname, record)
        print 'thread done ...'


def ProducerRecords():
    allnumber = 10000 * 100 * 2
    tbname = 'xxx'
    workers = []
    thread_count = 3

    queue = Queue.Queue()
    start_time = time.time()

    query_db = OperDb2('xxx', 52000, 'newdb2', 'xxx', 'xxx')

    # start thread
    for i in xrange(thread_count):
        workers.append(ConsumerRecords(queue))

    for i in xrange(thread_count):
        workers[i].start()

    # get source datalist
    with file(r'getRec.sql') as f:
        query_sql = f.read() % (tbname, allnumber)

    query_db.get_source_data(query_sql, queue)
    query_db.close_conn()

    # put quit flag
    for i in xrange(thread_count):
        queue.put('quit')

    # wait all thread exit
    for i in xrange(thread_count):
        workers[i].join()

    # print log & performance
    commit_num = 0
    while not log_queue.empty():
        tid, num, taken, tps = log_queue.get()
        print 'thread[{:10s}], num[{:6d}], taken[{:10s}], tps[{:10s}]'.format(tid, num, taken, tps)
        commit_num += int(num)

    print '-' * 50
    print 'Done {} time taken: {:0.6f}, avg TPS: {:0.6f}'.format(commit_num, time.time() - start_time,
                                                                 commit_num / (time.time() - start_time))

# main
if __name__ == '__main__':
    ProducerRecords()
