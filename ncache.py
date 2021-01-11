#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
In memory key value store based on python dict with TCP interface and
basic language parsing.
"""

__author__ = "Niall O'Connor zechs dot marquie at gmail dot com"
__version__ = '1.0'

from   datetime import datetime, timedelta
import logging
import socket
from   sys import getsizeof

# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

cache_timeouts = {}
cache_values = {}

def _get_or_timeout(key):
    """
    Returns a key or clears timed out key.

    ncache.cahce_values   contains key->value   pairs
    ncache.cache_timeouts contains key->timeout pairs

    There are two flavours of timeout
    1. ('ttl',  <ttl datetime>)
    2. ('perm', <created time>)

    Specification.

    | key | timeout type |    date      |  expected   |
    +-----+--------------+--------------+-------------+
    |'k1' |    'ttl'     | in future    |  value      |
    |'k2' |    'ttl'     | in past      | 'NOT FOUND' |
    |'k3' |    'perm'    | in past      |  value      |

    :param str key: A key in the timeout and value caches.
    """
    expiry = cache_timeouts.get(key, None)
    value = None
    if expiry:
        # We found a time to live key
        if expiry[0] == 'ttl':
            # ttl is in the future or else the key should be deleted.
            if expiry[1] > datetime.now():
                value =  cache_values.get(key, None)
        else: # key is permanent
            value = cache_values.get(key, None)
    if value is None:
        # no key could be returned so be sure both cache timeouts and cache values are in sync.
        cache_timeouts.pop(key, None)
        cache_values.pop(key, None)
        value = 'NOT FOUND'
    return value

def _clear_perm_keys(chunk_size):
    """
    Clears a chunk of the oldest perm keys to free space.

    :param int chunk_size: The number of keys to clear.
    """
    keys = [(key, val[1],) for key, val in cache_timeouts.items() if val[0] == 'perm']
    keys = sorted(keys, key=lambda x: x[1], reverse=False)
    keys = keys[:chunk_size]
    for key, _ in keys:
        cache_timeouts.pop(key, None)
        cache_values.pop(key, None)

def _clear_ttl_keys(future):
    """
    Clears all ttl keys who expire before the given future date.

    :param datetime.datetime future: Future date used to expire keys.
    """
    keys = [(key, val[1],) for key, val in cache_timeouts.items() if val[0] == 'ttl']
    for key, expiry in keys:
        if expiry < future:
            cache_timeouts.pop(key, None)
            cache_values.pop(key, None)

def _set_key(key, value, ttl=None):
    """
    Sets a key in cache_values and cache_timeouts.

    :param str key: The key we are setting.
    :param str val: The value we are setting.
    :param int ttl: The time to live in seconds. Optional.
    """
    if ttl is not None:
        cache_timeouts[key] = ('ttl', datetime.now() + timedelta(seconds=ttl),)
    else:
        cache_timeouts[key] = ('perm', datetime.now(),)
    cache_values[key] = value

def parse_command(command):
    """
    Parses SET and GET commands. Behaviour of the set command is represented below.

    SET <KEY_NAME> "<VALUE>" TTL=<int>
        - Key names CANNOT have spaces. TTL is optional.

    GET <KEY_NAME>
        - Key names CANNOT have spaces

    |   command               |  Expected              |
    +-------------------------+------------------------+
    | SET k1 "some test data" | k1 -> "some test data" |
    | SET k2 "some test data" | k2 -> "some test data" |
    | SET k2 "more test data" | k1 -> "more test data" |
    | SET k3 some test data   | Exception - wrong args |
    | SET k3 "me " TTL=20     | k3 -> "me "            |
    | SET k4                  | Exception - wrong args |
    | GET thing               | NOT FOUND              |
    | GET k3                  | "me "                  |
    """
    # preceeding and trailing spaces are removed and the entire string is split on spaces.
    command = command.lstrip(' ').rstrip(' ').split(' ')

    # A GET command can only be followed by a KEY
    if command[0].lower() == 'get': # case insensitive
        if len(command) == 2:
            return _get_or_timeout(command[1])
        else:
            raise ValueError('ERROR: Wrong number of args for GET. Received {0}, expected 2'.format(len(command)))
    # A SET command can be followed by a KEY, then a VALUE and finally an optional TTL
    elif command[0].lower() == 'set': # case insensitive
        # Do we have at least the right num of params?
        if len(command) > 2:
            # parse out the ttl
            ttl = command[-1].lower()
            if ttl.startswith('ttl=') and ttl[4:].isdigit():
                command = [command[0], command[1], ' '.join(command[2:-1]), command[-1]]
                ttl = int(ttl[4:])
            else:
                command = [command[0], command[1], ' '.join(command[2:])]
                ttl = None

            # examine the value and remove encapsulating quotes mess
            if command[2][0] == '"' and command[2][-1] == '"':
                command = command = [command[0], command[1], command[2][1:-1]]

            _set_key(command[1], command[2], ttl=ttl)
            return 'SUCCESS'
        else:
            raise ValueError('ERROR: Wrong number of args for SET. Received {0}, expected 2'.format(len(command)))
    else:
        raise ValueError('ERROR: Unkown command. Not GET or SET')

def manage_memory(chunk_size, step_seconds, limit):
    """
    Manage the size of the cache by first removing the oldest perm keys.
    If that fails to reduce the cache size then ttl keys that will expire before
    now + step_seconds will be removed.  Step_seconds is incremented to keep
    removing keys until the limit is respected.

    :param int chunk_size: The number of perm keys to remove.
    :param int step_seconds: The increment steps in seconds for removing ttl keys.
    :param int limit: The cache size limit in bytes.
    """
    # record the cache size in bytes and the ttl step in seconds
    old_cache_size = current_cache_size()
    this_step = step_seconds

    # While the cache is over the tolerance limit
    while current_cache_size() > limit:

        # Get rid of the oldest perm keys
        _clear_perm_keys(chunk_size)

        # If the cache size has not changed them all the perm keys have been removed
        if current_cache_size() == old_cache_size:
            # So while the cache is over the tolerance limit
            while current_cache_size() > limit:
                # start removing ttl keys that will in expire in this_step worth of seconds
                _clear_ttl_keys(this_step)
                # Increment this_step to keep removing more keys until the tolerance is preserved.
                this_step += step_seconds

def init_socket(ip, port):
    """
    Factory returns a tcp socet bound to ip and port

    :param str ip: ipaddress for service to run on
    :param int port: port for service to run on.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((ip, port,))
    s.listen(1)
    return s

current_cache_size = lambda : getsizeof(cache_values) + getsizeof(cache_timeouts)

def run_ncache(ip='127.0.0.1', port=5005, buffer_size=1024, max_memory=1933000000,
               memory_tolerance=.95, clear_perm_chunk=1, clear_ttl_step=300):
    """
    Creates and binds to a tcp socket to listen for cache commands.  Calculates
    memory limits.  Listens for and parses new commands.  Returns responses.

    :param str ip: The ip address to bind tcp socket to.
    :param int port: The port to bind tcp socket to.
    :param int buffer_size: Read this number of bytes from the tcp buffer. Smaller can be faster.
    :param int max_memory: The memory limit in bytes.
    :param float memory_tolerance: Precentage of max_memory that will be our limit, we may go over briefly.
    :param int clear_perm_chunk: The number of perm keys to remove.
    :param int clear_ttl_step: The increment steps in seconds for removing ttl keys.
    """
    s = init_socket(ip, port)
    conn, addr = s.accept()

    logger.info("Connection Address: %s", addr)

    limit = int(max_memory*memory_tolerance)

    while True:
        data = conn.recv(buffer_size)
        if not data:
            break
        try: # execute
            ### Manage memory before we add more keys
            manage_memory(clear_perm_chunk, clear_ttl_step, limit)

            ### Parse the command and return the response.
            response = parse_command(data)
            conn.send(response)

        except ValueError as e:
            ### If a parse error occured
            logger.exception(e)
            conn.send(str(e))
    conn.close()
