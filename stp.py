#!/usr/bin/env python
# coding: utf-8

import os, stat, glob, sys
import signal, logging
import urllib2, socket
import threading, Queue
import datetime, time
import json
from SocketServer import ThreadingMixIn
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

def signal_handler(signal, frame):
    logging.info('Ctrl+C pressed. Stopping services...')
    sched.terminate()
    time.sleep(1)
    sys.exit(0)

class Scheduler:
    def __init__(self, queue):
        self.queue = queue
        self._terminate = threading.Event()
        self._last_check = time.localtime(time.time()-60)
    
    def terminate(self):
        logging.info('scheduler: received termination request')
        self._terminate.set()

    def run(self):
        now = time.localtime(time.time())
        if now.tm_min != self._last_check.tm_min:
            for k, v in stantions.items():
                if k == 'common':
                    continue
                now = time.localtime(time.time()) if 'shift' not in v else time.localtime(time.time()+(int(v['shift'])*3600))
                d = [int(x) for x in v['day'].split(',')]
                if  now.tm_hour == int(v['hour']) and now.tm_min == int(v['min']) and now.tm_wday in d:
                    logging.info(u'scheduling: activated for ' + k)
                    newthread = DownloadThread(v['url'], stantions['common']['folder'], v['id'], int(v['duration'])*60, self.queue, self._terminate)
                    newthread.daemon = True
                    newthread.start()
        self._last_check = now

class DownloadThread(threading.Thread):
    def __init__(self, stream_url, dname, feed_id, duration, queue, terminate):
        self.stream_url = stream_url
        fname = '%s_%s.mp3' % (datetime.datetime.now().strftime('%Y-%m-%d_%H-%M'), feed_id)
        self.fname = os.path.join(dname, fname)
        self.duration = duration
        self.queue = queue
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
        self.queue.put(self.fname)
        logging.info(u'Записан файл ' + self.fname)

class Config:
    def __init__(self, fname):
        self.fname = fname
        self._lastmtime = 0

    def load(self):
        self._lastmtime = os.stat(self.fname)[stat.ST_MTIME]
        return json.load(open(self.fname))

    def is_updated(self):
        return (self._lastmtime != os.stat(self.fname)[stat.ST_MTIME])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        logging.info('web-server: GET request. src=%s params=%s' % (self.client_address,  self.path))
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write('<a href="http://localhost:8082/mp3/rss.xml">RSS</a>')

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

# Переделать в класс, чтобы работало сообщение terminate
def serve_on_port(port):
    server = ThreadingHTTPServer(('localhost', port), Handler)
    server.serve_forever()

class RSS:
    def __init__(self, folder, ip, port):
        self.folder = folder
        self.ip = ip
        self.port = port
        self.rss_top = '''
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
    <title>Stream-to-Pocast</title>
    <description>generated by STP</description>
    <link>http://%s:%d</link>
    <language>ru-RU</language>
    <lastBuildDate>%s</lastBuildDate>'''
        self.rss_item = '''
<item>
    <title>%s</title>
    <link>/%s</link>
    <guid>%s.uid</guid>
    <description>generated by STP</description>
    <enclosure url="http://%s:%d/%s" length="%d" type="audio/mpeg"/>
    <pubDate>%s</pubDate>
</item>'''
        self.rss_bottom = '</channel></rss>'

    def search_files(self):
        return glob.iglob(os.path.join(self.folder, '*.mp3'))

    def save(self, content):
        with open(os.path.join(self.folder, 'rss.xml'), 'w') as f:
            f.write(content)

    def generate(self):
        logging.info('rss: regenerate')
        rfc822 = '%a, %d %b %Y %H:%M:%S +0000'
        cur_date = time.strftime(rfc822, time.localtime())
        rss = [self.rss_top % (self.ip, self.port, cur_date)]
        for f in self.search_files():
            ftime = time.strftime(rfc822, time.localtime(os.stat(f)[stat.ST_MTIME]))
            fname = os.path.basename(f)
            flen = os.path.getsize(f)
            rss.append(self.rss_item % (fname, fname, fname, self.ip, self.port, fname, flen, ftime))
        rss.append(self.rss_bottom)
        rss = '\n'.join(rss)
        self.save(rss)

def clear_warehouse(folder, store):
    if store == 0:
        return
    for f in glob.glob(os.path.join(folder, '*.mp3')):
        fmtime = os.path.getmtime(f)
        if (time.time() - fmtime) > store*3600*24:
            try:
                os.remove(f)
                logging.info("Obsolete file %s deleted" % f)
            except:
                logging.info("Can't deleted obsolete file %s " % f)

if __name__ == '__main__':
    logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s]  %(message)s',
                        level = logging.DEBUG,
                        filename = __name__ + '.log')
    logging.info('STP started')
    signal.signal(signal.SIGINT, signal_handler)

    conf = Config('stations.json')
    stantions = conf.load()
    
    queue = Queue.Queue()
    sched = Scheduler(queue)
    
    port = 8082
    logging.info('web-server: started on localhost:%d' % port)
    serv = threading.Thread(target=serve_on_port, args=[port])
    serv.daemon = True
    serv.start()

    rss = RSS(stantions['common']['folder'], 'localhost', port)
    rss.generate()

    while True:
        if conf.is_updated():
            logging.info('STP config updated')
            stantions = conf.load()
        sched.run()
        if not queue.empty():
            fname = queue.get()
            logging.info('STP: New file [%s] detected' % fname)
            clear_warehouse(stantions['common']['folder'], int(stantions['common']['store']))
            rss.generate()
        time.sleep(1)