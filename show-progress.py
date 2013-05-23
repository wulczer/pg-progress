#!/usr/bin/python
import argparse
import glob
import math
import multiprocessing
import os
import psycopg2
import Queue
import subprocess
import sys
import tempfile
import time

try:
    from PyQt4 import QtGui, QtCore
except ImportError:
    pass


def query(connstr, sql, waitq, q):
    conn = psycopg2.connect(connstr)
    cur = conn.cursor()
    pid = conn.get_backend_pid()

    sys.stderr.write('[Q] query connection opened with pid %d\n' % pid)

    q.put(pid)
    q.get()

    sys.stderr.write('[Q] start query executing\n')

    cur.execute(sql)

    conn.close()
    q.put(None)
    waitq.get()
    sys.stderr.write('[Q] query done\n')


def monitor(connstr, interval, outputdir, dot, waitq, reportq, q):
    query_pid = q.get()

    conn = psycopg2.connect(connstr)
    cur = conn.cursor()
    pid = conn.get_backend_pid()

    sys.stderr.write('[M] monitor connection opened with pid %d\n' % pid)

    waitq.get()
    q.put(None)

    snapshots = []
    while True:
        time.sleep(interval)
        cur.execute('select pg_progress_update(%s)', (query_pid, ))

        if dot:
            cur.execute('select pg_progress_dot()')
            snapshots.append(cur.fetchone()[0])

        cur.execute('select pg_progress()')
        reportq.put(cur.fetchone()[0])

        try:
            q.get_nowait()
            break
        except Queue.Empty:
            pass

    reportq.put(None)
    waitq.get()

    if dot:
        sys.stderr.write('[M] writing snapshots\n')

    for i, snapshot in enumerate(snapshots):
        if not snapshot:
            continue
        fname = os.path.join(outputdir, '%04d_snapshot.dot' % i)
        with open(fname, 'w') as f:
            f.write(snapshot)

    conn.close()
    sys.stderr.write('[M] monitoring done\n')


def process_dot_files(opts):
    sys.stderr.write('[*] processing dot files\n')

    fnames = list(sorted(glob.glob(os.path.join(opts.output, '*.dot'))))

    if not fnames:
        sys.stderr.write('[*] no snapshots\n')
        return

    cmd = ['dot', '-Tpng', '-O'] + fnames
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)

    for i, fname in enumerate(fnames, 1):
        pngname = os.extsep.join((fname, 'png'))
        temp = tempfile.NamedTemporaryFile()
        cmd = ['convert', '-gravity', 'northwest',
               '-pointsize', '30',
               '-splice', '40x40', '-annotate', '+20+20',
               '%d/%d' % (i, len(fnames)), pngname, temp.name]
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)

        temp.seek(0)
        open(pngname, 'w').write(temp.read())
        temp.close()

    cmd = ['convert']
    for fname in fnames:
        pngname = os.extsep.join((fname, 'png'))
        cmd.extend(['-delay', '20', pngname])
    cmd.append(os.path.join(opts.output, 'snapshot.gif'))
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)


class TextReporter(object):

    BAR_LENGTH = 120

    def start(self, sql):
        pass

    def progress(self, progress):
        bar_length = math.floor(progress * self.BAR_LENGTH)
        bar_length = min(bar_length, self.BAR_LENGTH)
        bar = ('#' * int(bar_length)).ljust(self.BAR_LENGTH)

        sys.stdout.write('\r[%s] %.5f ' % (bar, progress))
        sys.stdout.flush()

    def stop(self):
        sys.stdout.write('\n')


class GraphicalReporter(object):

    BAR_LENGTH = 100000

    def start(self, sql):
        self.app = QtGui.QApplication([])
        self.dialog = QtGui.QDialog()

        self.label = QtGui.QLabel()
        self.bar = QtGui.QProgressBar()
        self.query = QtGui.QLabel(sql)

        layout = QtGui.QVBoxLayout()
        layout.addWidget(self.query)
        layout.addWidget(self.bar)
        layout.addWidget(self.label)

        self.dialog.setLayout(layout)
        self.dialog.setMinimumSize(800, 0)

        self.bar.setRange(0, self.BAR_LENGTH)
        self.bar.setMinimumSize(0, 100)
        self.bar.setStyleSheet('font-size: 46px')

        self.query.setStyleSheet('font-size: 46px; padding-bottom: 40px')
        self.query.setAlignment(QtCore.Qt.AlignHCenter)

        self.label.setStyleSheet('font-size: 46px; padding-top: 40px')
        self.label.setAlignment(QtCore.Qt.AlignHCenter)

        self.progress(0.0)
        self.dialog.show()

    def progress(self, progress):
        bar_length = math.floor(progress * self.BAR_LENGTH)
        bar_length = min(bar_length, self.BAR_LENGTH)

        self.label.setText('%.5f' % progress)
        self.bar.setValue(int(bar_length))

    def stop(self):
        self.dialog.close()
        self.app.exit()


def main():
    parser = argparse.ArgumentParser(
        description='run a query and monitor progress')
    parser.add_argument('-c', '--command', required=True,
                        help='the query to run')
    parser.add_argument('-g', '--graphical', action='store_true', default=False,
                        help='show a graphical progress bar')
    parser.add_argument('-d', '--dot', action='store_true', default=False,
                        help='produce a series of GraphViz snapshots')
    parser.add_argument('-i', '--interval', default=0.5, type=float,
                        help='monitoring snapshots interval, defaults to 0.5s')
    parser.add_argument('-o', '--output', default='.',
                        help=('output directory for GraphViz snapshots, '
                              'defaults to current'))
    parser.add_argument('-W', '--wait', action='store_true', default=False,
                        help=('wait for keyboard input before running queries '
                              'to allow a debugger to be attached'))
    parser.add_argument('connstr', nargs='?', help='connection string', default='')

    opts = parser.parse_args()

    q = multiprocessing.Queue()
    reportq = multiprocessing.Queue()
    waitq = multiprocessing.Queue()

    if not opts.wait:
        waitq.put(None)

    queryp = multiprocessing.Process(target=query, args=(
            opts.connstr, opts.command, waitq, q))
    monitorp = multiprocessing.Process(target=monitor, args=(
            opts.connstr, opts.interval, opts.output, opts.dot,
            waitq, reportq, q))

    reporter = TextReporter()
    if opts.graphical:
        reporter = GraphicalReporter()

    sys.stderr.write('[*] starting query and monitor processes\n')

    queryp.start()
    monitorp.start()

    if opts.wait:
        raw_input('waiting for keyboard input... ')
        waitq.put(None)

    reporter.start(opts.command)

    while True:
        progress = reportq.get()
        if progress is None:
            break
        reporter.progress(progress)

    reporter.stop()

    waitq.put(None)
    waitq.put(None)

    queryp.join()
    monitorp.join()

    if opts.dot:
        process_dot_files(opts)

    sys.stderr.write('[*] done\n')


if __name__ == '__main__':
    main()
