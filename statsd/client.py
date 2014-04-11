from __future__ import with_statement
import cPickle
import datetime
from functools import wraps
import logging
import random
import socket
import struct
import time


__all__ = ['StatsClient']


_utc_epoch = datetime.datetime.utcfromtimestamp(0)


def _utc_now():
    return int(datetime.datetime.utcnow() - epoch).total_seconds())


class Timer(object):
    """A context manager/decorator for statsd.timing()."""

    def __init__(self, client, stat, rate=1):
        self.client = client
        self.stat = stat
        self.rate = rate
        self.ms = None
        self._sent = False
        self._start_time = None

    def __call__(self, f):
        @wraps(f)
        def wrapper(*args, **kw):
            with self:
                return f(*args, **kw)
        return wrapper

    def __enter__(self):
        return self.start()

    def __exit__(self, typ, value, tb):
        self.stop()

    def start(self):
        self.ms = None
        self._sent = False
        self._start_time = time.time()
        return self

    def stop(self, send=True):
        if self._start_time is None:
            raise RuntimeError('Timer has not started.')
        dt = time.time() - self._start_time
        self.ms = int(round(1000 * dt))  # Convert to milliseconds.
        if send:
            self.send()
        return self

    def send(self):
        if self.ms is None:
            raise RuntimeError('No data recorded.')
        if self._sent:
            raise RuntimeError('Already sent data.')
        self._sent = True
        self.client.timing(self.stat, self.ms, self.rate)


class StatsClient(object):
    """A client for statsd."""

    def __init__(self, host='localhost', port=8125, prefix=None,
                 maxudpsize=512):
        """Create a new client."""
        self._addr = (socket.gethostbyname(host), port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._prefix = prefix
        self._maxudpsize = maxudpsize

    def pipeline(self):
        return Pipeline(self)

    def carbon_pipeline(self):
        return CarbonPipeline(self)

    def pickle_pipeline(self):
        return PicklePipeline(self)

    def timer(self, stat, rate=1):
        return Timer(self, stat, rate)

    def timing(self, stat, delta, rate=1):
        """Send new timing information. `delta` is in milliseconds."""
        self._send_stat(stat, '%d|ms' % delta, rate)

    def incr(self, stat, count=1, rate=1):
        """Increment a stat by `count`."""
        self._send_stat(stat, '%s|c' % count, rate)

    def decr(self, stat, count=1, rate=1):
        """Decrement a stat by `count`."""
        self.incr(stat, -count, rate)

    def gauge(self, stat, value, rate=1, delta=False):
        """Set a gauge value."""
        if value < 0 and not delta:
            if rate < 1:
                if random.random() > rate:
                    return
            with self.pipeline() as pipe:
                pipe._send_stat(stat, '0|g', 1)
                pipe._send_stat(stat, '%s|g' % value, 1)
        else:
            prefix = '+' if delta and value >= 0 else ''
            self._send_stat(stat, '%s%s|g' % (prefix, value), rate)

    def set(self, stat, value, rate=1):
        """Set a set value."""
        self._send_stat(stat, '%s|s' % value, rate)

    def raw_value(self, stat, value):
        """Send a raw value.

        For when you skip statsd and talk straight to carbon.  Because
        carbon doesn't support rate, neither does raw_value.
        """
        self._send_stat(stat, value, 1)

    def _send_stat(self, stat, value, rate):
        self._after(self._prepare(stat, value, rate))

    def _prepare(self, stat, value, rate):
        if rate < 1:
            if random.random() > rate:
                return
            value = '%s|@%s' % (value, rate)

        if self._prefix:
            stat = '%s.%s' % (self._prefix, stat)

        return '%s:%s' % (stat, value)

    def _after(self, data):
        if data:
            self._send(data.encode('ascii'))

    def _send(self, data):
        """Send data to statsd."""
        try:
            self._sock.sendto(data, self._addr)
        except socket.error:
            # No time for love, Dr. Jones!
            pass


class Pipeline(StatsClient):
    def __init__(self, client):
        self._client = client
        self._prefix = client._prefix
        self._maxudpsize = client._maxudpsize
        self._stats = []

    def _after(self, data):
        if data is not None:
            self._stats.append(data)

    def __enter__(self):
        return self

    def __exit__(self, typ, value, tb):
        self.send()

    def send(self):
        # Use pop(0) to preserve the order of the stats.
        if not self._stats:
            return
        data = self._stats.pop(0)
        while self._stats:
            stat = self._stats.pop(0)
            if len(stat) + len(data) + 1 >= self._maxudpsize:
                self._client._after(data)
                data = stat
            else:
                data += '\n' + stat
        self._client._after(data)


class CarbonPipeline(Pipeline):
    """Like pipeline, but talks to carbon instead of statsd.

    Note that you should only use raw_value() to set values in this protocol.
    """
    def _prepare(self, stat, value, rate):
        assert rate == 1, 'Cannot use sample-rates with CarbonPipeline'

        if self._prefix:
            stat = '%s.%s' % (self._prefix, stat)

        # hostedgraphite doesn't require the timestamp (it uses the
        # current time if it's absent).  That should be good enough
        # for us; let's save some space!
        return '%s %s' % (stat, value)


class PicklePipeline(Pipeline):
    """Like pipeline, but uses the pickle protocol to talk to carbon.

    Note that you should only use raw_value() to set values in this protocol.
    """
    def _prepare(self, stat, value, rate):
        assert rate == 1, 'Cannot use sample-rates with PicklePipeline'

        if self._prefix:
            stat = '%s.%s' % (self._prefix, stat)

        return (stat, (value, _utc_now()))

    def _pickle(self, data):
        payload = cPickle.dumps(data, protocol=cPickle.HIGHEST_PROTOCOL)
        header = struct.pack("!L", len(payload))
        return header + payload

    def send(self):
        # Use linear search to figure out how much of the stats we can
        # send in one go.  (We could be more efficient if we ever
        # needed to.)
        i = len(self._stats)
        while self._stats:
            pickled_data = self._pickle(self._stats[:i])
            # If i == 1, then data[0] is too big to fit in a packet on
            # its own.  We try to send it anyway, hoping our guess of
            # maxudpsize was too conservative.
            if len(pickled_data) < self._maxudpsize or i == 1:
                self._client._send(pickled_data)
                del self._stats[:i]
                i = len(self._stats)
            else:
                i -= 1
