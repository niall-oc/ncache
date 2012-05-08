#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Niall O'Connor zechs dot marquie at gmail dot com"
__version__ = '1.0'

from hashlib import sha1
import redis
import json
import cPickle as pickle
from functools import wraps

#Redis cache for this module
r_conn = redis.Redis(host='127.0.0.1', port=6379, db=0)

#####################################################################
##########           Caching decorator            ###################
#####################################################################

def cachable(seconds=None, prefix='', key_name=None, hash_keys=False, pickled=True):
    """
    Decorate an expensive calculation to save on computing. All values are 
    pickled before being stored.  Keys may be hashed for some security.
    
    :param seconds:  Number of seconds to live in the cache
    :type seconds:   Int str float or None
    :param prefix:   A prefix for a key.
    :type prefix:    str
    :param key_name: The specific name for this key. If omitted this key
                     will be made of the calling function name and a list of 
                     its args.
    :type key_name:  str or None
    :param hash_keys: Activates key hashing. Prefixes are not hashed.
    :type hash_keys: bool
    :param pickled:  Sets pickling of objects before storing.
    :type pickled:   bool
    
    """
    def collect(f):
        @wraps(f)
        def do_caching(*args, **kw):
            fargs = "%s_%s_%s"%(f.__name__, args, kw)
            fargs = sha1(fargs).hexdigest() if hash_keys else fargs
            key = prefix+key_name if key_name else prefix + fargs
            if key in r_conn:
                try:
                    return pickle.loads(r_conn.get(key))
                except cPickle.UnpicklingError:
                    return r_conn.get(key)
            result = f(*args, **kw)
            value = pickle.dumps(result) if pickled else result
            if seconds:
                r_conn.setex(key, value, seconds)
            else:
                r_conn.set(key, value)
            return result
        return do_caching
    return collect

    
