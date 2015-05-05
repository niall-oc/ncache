#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
In memory key value store based on python dict with TCP interface and
basic language parsing.
"""

__author__ = "Niall O'Connor zechs dot marquie at gmail"
__version__ = "1.0"

import cPickle
from   functools import wraps
from   hashlib import sha1
import logging
import re
import socket
import warnings

logger = logging.getLogger(__name__)

def func2key(func, *args, **kw):
    """
    Convert a function and its arguments to a cache key.
    """
    kw = kw.items() # dicts have no order so converting to a list of tuples and sort
    kw.sort()
    return sha1('%s_%s_%s' % (func.__name__, args, kw)).hexdigest()

class NCache(object):
    def __init__(self, ip='127.0.0.1', port=5005, buffer=1024, key_rexp="[\w\d-]{2,}", pickled=True):
        """
        Create a new cache key.

        :param str ip: Ip address of tcp server.
        :param str port: Port number of tcp server.
        :param int buffer: TCP buffer size.
        :param str key_regx: Key names must match this regex.
        :param bool pickled: Pickle data when recording them.
        usage:
            >>> my_cache = NCache()
            >>> my_cache.set('Something', 'Not nothing')
            >>> my_cache.get('Something')
            Not nothing
        """
        super(NCache, self).__init__()
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect((ip, port,))
        self.buffer_size = buffer
        self.pickled = pickled
        self.__rexp = re.compile(key_rexp)
        self.__key_rexp = key_rexp

    def _execute_command(self, command):
        """
        Execute the constructed command and handle the response.
        """
        self.conn.send(command)
        response = self.conn.recv(self.buffer_size)
        if response.startswith('ERROR: '):
            raise ValueError(response)
        return response

    def cachable(self, key_name=None, seconds=None, overwrite=True, cache_until=None):
        """
        Decorate an expensive calculation to save on computing. All values are
        pickled before being stored. Keys may be hashed for some security. The may
        be prefixed for easy lookups.
        Note - Only cache module methods. To cache class methods, specify a key_name.
                Additional fix/support is needed for caching class methods

        :param str key_name: The specific name for this key. If omitted this key will be made of the calling function name and a list of its args.
        :param int seconds: The expiry time of this key
        :param bool overwrite: A flag to set whether a key may be overwritten
        :param datetime.datetime cache_until: Datetime that this key will expire on

        Usage:
            >>> cache = Cache()
            >>> @cache.cachable()
                def addit(a, b):
                    return a+b
        """
        def collect(f):
            # using functools.wrap allows all func parametres to be passed to this decorator
            # while preserving the doc strings and other meta data.
            @wraps(f)
            def do_caching(*args, **kw):
                # key is hashed by func2key
                key = func2key(f, *args, **kw) if key_name is None else key_name
                value = self.get(key, hash=False)
                if not value: # If value is none nothing exists and we must call the decorated function
                    value = f(*args, **kw)
                    if value is None:
                        # You shouldn't decorate fuctions that retrun None with a cache decorator.
                        # The warning is helpfully printed before the raise statement.
                        warnings.warn("""

                            Decorated function must exit using the return keyword and must NOT return None.

                            Instead return something similar but meaningful in the context of your function eg:
                            [], {}, 0, False, str("None"), str("No records") etc.""")
                        raise TypeError('NoneType is not cachable. If required None can be cached using Cache.set()')

                    self.set(key, value, seconds=seconds)
                return value
            return do_caching
        return collect

    def __validate_key(self, key):
        """
        Ensures a key name passes the regex check with expression in self.__key_rexp
        Raises and exception if the key cannot pass the regular expression check.

        :param str key: A key name.
        """
        is_match = self.__rexp.match(key)
        if not is_match or is_match.group() is not key:
            raise KeyError('"%s" is an invalid key as it does not match with the following regular expression, %s'%(key, self.__key_rexp))
        return key

    def set(self, key, value, seconds=None):
        """
        Set a key in the cache.

        :param str key: The specific name for this key.
        :param object value: The object to be cached.
        :param int seconds: The expiry time of this key.
        """
        key = self.__validate_key(key)
        if self.pickled:
            value = cPickle.dumps(value)
        ttl = ""
        if seconds is not None:
            ttl = "TTL={0}".format(seconds)
        command = "SET {0} {1} {2}".format(key, value, ttl)
        return self._execute_command(command)

    def get(self, key):
        """
        Get a key from the cache

        :param str key: The specific name for this key.
        """
        key = self.__validate_key(key)
        command = "GET {0}".format(key)
        resp = self._execute_command(command)
        if resp == 'NOT FOUND':
            resp = None
        elif self.pickled:
            resp = cPickle.loads(resp)
        return resp
