#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Tests for ncache"""

__author__ = "Niall O'Connor"

from   datetime import datetime, timedelta
from   playground.nialloc.stuff.dimcs import ncache
import unittest2 as unittest

class TestNCacheGetOrTimeout(unittest.TestCase):
    """
    ncache.get_or_timeout works the following way.

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
    """

    def test_get_or_timeout_fresh_key(self):
        """
        In this case a key has a time to live in the future and should return data.
        """
        ncache.cache_timeouts['TEST1'] = ('ttl', datetime.now() + timedelta(days=10),)
        ncache.cache_values['TEST1'] = 'some test data'

        data = ncache._get_or_timeout('TEST1')
        self.assertEqual(data, 'some test data')

    def test_get_or_timeout_stale_key(self):
        """
        In this case a key has a time to live in the past and should return NOT FOUND.
        """
        ncache.cache_timeouts['TEST1'] = ('ttl', datetime.now() - timedelta(days=10),)
        ncache.cache_values['TEST1'] = 'some test data'

        data = ncache._get_or_timeout('TEST1')
        self.assertEqual(data, 'NOT FOUND')

    def test_get_or_timeout_perm_key(self):
        """
        In this case a key is perm and should return data.
        """
        ncache.cache_timeouts['TEST1'] = ('perm', datetime.now() - timedelta(days=10),)
        ncache.cache_values['TEST1'] = 'some test data'

        data = ncache._get_or_timeout('TEST1')
        self.assertEqual(data, 'some test data')


class TestNCacheClearKeys(unittest.TestCase):
    """
    Occasionally memory limits will force a clearout of ttl expired keys or perm keys

    Rules for clearing keys.

    1. Clear the oldest chunk of perm keys while cache is still full.
    2. If all perm keys have been deleted clear ttl keys that will expire in 5 mins.
    3. If cache is still full increase expire time by 5 mins and clear ttl keys,
       repeat this step until cache is clear.


    """
    def test_clear_perm_keys(self):
        """
        Given the following

        | key | timeout type |    date      |
        +-----+--------------+--------------+
        |'k1' |    'perm'    | now - 100sec |
        |'k2' |    'perm'    | now - 200sec |
        |'k3' |    'perm'    | now - 10sec  |

        order of deletion should be k2, then k1, then k3
        """
        ncache.cache_timeouts['k1'] = ('perm', datetime.now() - timedelta(seconds=100),)
        ncache.cache_values['k1']   = 'some test data'
        ncache.cache_timeouts['k2'] = ('perm', datetime.now() - timedelta(seconds=200),)
        ncache.cache_values['k2']   = 'some test data'
        ncache.cache_timeouts['k3'] = ('perm', datetime.now() - timedelta(seconds=10),)
        ncache.cache_values['k3']   = 'some test data'

        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'some test data')

        ncache._clear_perm_keys(1)
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'some test data')

        ncache._clear_perm_keys(1)
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'some test data')

        ncache._clear_perm_keys(1)
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'NOT FOUND')

    def test_clear_ttl_keys(self):
        """
        Given the following

        | key | timeout type |    date      |
        +-----+--------------+--------------+
        |'k1' |    'ttl'    | now + 360sec  |
        |'k2' |    'ttl'    | now + 640sec  |
        |'k3' |    'ttl'    | now + 720sec  |

        step 5 mins or 300 seconds forward to remove no keys
        step 5 mins or 300 seconds forward to remove k1
        step 5 mins or 300 seconds forward to remove k3 and k2
        """
        ncache.cache_timeouts['k1'] = ('ttl', datetime.now() + timedelta(seconds=360),)
        ncache.cache_values['k1']   = 'some test data'
        ncache.cache_timeouts['k2'] = ('ttl', datetime.now() + timedelta(seconds=640),)
        ncache.cache_values['k2']   = 'some test data'
        ncache.cache_timeouts['k3'] = ('ttl', datetime.now() + timedelta(seconds=720),)
        ncache.cache_values['k3']   = 'some test data'

        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'some test data')

        future = datetime.now() + timedelta(seconds=300)
        ncache._clear_ttl_keys(future)
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'some test data')

        future = future + timedelta(seconds=300)
        ncache._clear_ttl_keys(future)
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'some test data')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'some test data')

        future = future + timedelta(seconds=300)
        ncache._clear_ttl_keys(future)
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'NOT FOUND')


class TestNCacheSetKeys(unittest.TestCase):
    """
    When a cache key is set, its set.  You can get it back out provided it is not expired!

    We can assert the key is present in both the values and timeout caches.
    We can assert the key holds the correct data.
    We can never be absolutely certain about the expiry time, but its close.
    """
    def test_set_key_perm(self):
        """
        Assert 'k1' -> 'some_test_data' in ncache.cache_values
        Assert 'k1' -> ('perm', <datetime>)
        """
        ncache._set_key('k1', 'some_test_data')
        data = ncache.cache_timeouts.get('k1')
        self.assertEqual(data[0], 'perm')
        self.assertTrue(isinstance(data[1], datetime))
        self.assertTrue(bool(datetime.now() >= data[1]))
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some_test_data')
        # repeat test to guard against accidental removal using pop instead of get :-$
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some_test_data')

    def test_set_key_ttl(self):
        """
        Assert 'k1' -> 'some_test_data' in ncache.cache_values
        Assert 'k1' -> ('ttl', <datetime + 2000seconds>)
        """
        ncache._set_key('k1', 'some_test_data', ttl=2000)
        #XXX: hopefully this test will not stall over 2000seconds.  It wouldn't in pure python outside of quartz
        data = ncache.cache_timeouts.get('k1')
        self.assertEqual(data[0], 'ttl')
        self.assertTrue(isinstance(data[1], datetime))
        self.assertTrue(bool(datetime.now() <= data[1]))
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some_test_data')
        # repeat test to guard against accidental removal using pop instead of get :-$
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some_test_data')

    def test_set_key_ttl_expiry(self):
        """
        Assert 'k1' -> 'some_test_data' in ncache.cache_values
        Assert 'k1' -> ('ttl', <datetime + 0seconds>)
        """
        ncache._set_key('k1', 'some_test_data', ttl=0)
        data = ncache.cache_timeouts.get('k1')
        self.assertEqual(data[0], 'ttl')
        self.assertTrue(isinstance(data[1], datetime))
        self.assertTrue(bool(datetime.now() >= data[1]))
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'NOT FOUND')


class TestNCacheParseCommand(unittest.TestCase):
    """
    Following the simplicity of memcached, there are two commands. SET and GET.

    Syntax is.

    SET <KEY_NAME> "<VALUE>" TTL=<int>
        - Key names CANNOT have spaces. TTL is optional.

    GET <KEY_NAME>
        - Key names CANNOT have spaces
    """
    def test_parse_command_set(self):
        """
        Behaviour of the set command is represented below.

        |   command               |  Expected              |
        +-------------------------+------------------------+
        | SET k1 "some test data" | k1 -> "some test data" |
        | SET k2 "some test data" | k2 -> "some test data" |
        | SET k2 more test data   | k2 -> "more test data" |
        | SET k3 "me " TTL=20     | k3 -> "me "            |
        | SET k4                  | Exception - wrong args |
        | SET                     | Exception - wrong args |
        """
        status = ncache.parse_command('SET k1 "some test data"')
        self.assertEqual(status, 'SUCCESS')
        data = ncache._get_or_timeout('k1')
        self.assertEqual(data, 'some test data')

        status = ncache.parse_command('SET k2 "some test data"')
        self.assertEqual(status, 'SUCCESS')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'some test data')

        status = ncache.parse_command('SET k2 more test data')
        self.assertEqual(status, 'SUCCESS')
        data = ncache._get_or_timeout('k2')
        self.assertEqual(data, 'more test data')

        status = ncache.parse_command('SET k3 "me " TTL=20')
        self.assertEqual(status, 'SUCCESS')
        data = ncache._get_or_timeout('k3')
        self.assertEqual(data, 'me ')

        with self.assertRaises(ValueError):
            ncache.parse_command('SET k4')
        with self.assertRaises(ValueError):
            ncache.parse_command('SET')

    def test_parse_command_get(self):
        """
        Behaviour of the set command is represented below.

        |   command               |  Expected              |
        +-------------------------+------------------------+
        | SET k3 "me "            | k3 -> "me "            |
        | GET thing               | NOT FOUND              |
        | GET k3                  | "me "                  |
        | GET                     | Exception - wrong args |
        | GET my head             | Exception - wrong args |
        """
        status = ncache.parse_command('SET k3 "me "')
        self.assertEqual(status, 'SUCCESS')
        data = ncache.parse_command('GET thing')
        self.assertEqual(data, 'NOT FOUND')
        data = ncache.parse_command('GET k3')
        self.assertEqual(data, 'me ')

        with self.assertRaises(ValueError):
            ncache.parse_command('GET')
        with self.assertRaises(ValueError):
            ncache.parse_command('GET my head')

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestNCacheGetOrTimeout))
    suite.addTest(unittest.makeSuite(TestNCacheParseCommand))
    suite.addTest(unittest.makeSuite(TestNCacheClearKeys))
    suite.addTest(unittest.makeSuite(TestNCacheSetKeys))
    unittest.TextTestRunner().run(suite)
