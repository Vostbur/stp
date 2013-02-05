#!/usr/bin/env python
# coding: utf-8

import os
import stat
import signal
import sys
import urllib2, socket
import logging
import threading
import time
import json

def signal_handler(signal, frame):
    logging.info('Ctrl+C pressed. Stopping services...')
    sched.terminate()
    time.sleep(1)
    sys.exit(0)

class Scheduler:
    def __init__(self):
        self._terminate = threading.Event()
        self._last_check = time.localtime(time.time()-60)
    
    def terminate(self):
        logging.info('Scheduler received termination request')
        self._terminate.set()

    def run(self):
        now = time.localtime(time.time())
        if now.tm_min != self._last_check.tm_min:
            for k, v in stantions.items():
                if int(v['hour']) == now.tm_hour and int(v['min']) == now.tm_min:
                    logging.info('Scheduling activated for ' + v['file'])
                    newthread = DownloadThread(v['url'], v['file'], int(v['duration'])*60, self._terminate)
                    newthread.daemon = True
                    newthread.start()
        self._last_check = now

class DownloadThread(threading.Thread):
    def __init__(self, stream_url, fname, duration, terminate):
        self.stream_url = stream_url
        self.fname = fname
        self.duration = duration
        self._terminate = terminate
        self.buf_size = 4096
        self.conn = None
        self.reconnection = 9
        threading.Thread.__init__(self)

    def connect(self):
        try:
            self.conn = urllib2.urlopen(self.stream_url, timeout=10)
        except:
            logging.info('Error in open %s. Trying %s times' % (self.stream_url, self.reconnection))
            self.conn = None
        
    def run(self):
        target = open(self.fname, "wb")
        timestamp = time.time()
        self.connect()
        while (time.time() - timestamp) < self.duration and not self._terminate.is_set():
            if self.conn:
                try:
                    target.write(self.conn.read(self.buf_size))
                except socket.timeout:
                    logging.info('Timeout 10 sec. Error read from ' + self.stream_url)
            else:
                time.sleep(1)
                if self.reconnection:
                    self.reconnection -= 1
                    self.connect()
                else:
                    continue
        logging.info('FINISH CAPTURE TO ' + self.fname)

class Config:
    def __init__(self, fname):
        self.fname = fname
        self._lastmtime = 0

    def load(self):
        self._lastmtime = os.stat(self.fname)[stat.ST_MTIME]
        return json.load(open(self.fname))

    def is_updated(self):
        return (self._lastmtime != os.stat(self.fname)[stat.ST_MTIME])

if __name__ == '__main__':
    logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                        level = logging.DEBUG,
                        filename = __name__ + '.log')
    logging.info('STP started')
    signal.signal(signal.SIGINT, signal_handler)

    conf = Config('stations.json')
    stantions = conf.load()
    sched = Scheduler()
    
    while True:
        if conf.is_updated():
            logging.info('STP config updated')
            stantions = conf.load()
        sched.run()
        time.sleep(1)