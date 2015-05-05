ncache
======

Its too slow to do this in python.  Its not really a production ready thing.  So what is the point?  Well its easy to Test/Spec things in python and its easy to write code in python.  So you can potentially build something like memcached again and understand how it works.

Run server
----------
```python
>>> from ncache import run_ncache
>>> run_ncache()
```
Run client
----------
```python
>>> from ncache_client import NCache
>>> cache = NCache()
>>> cache.set('hello', 'world')
>>> cache.get('hello')
world
>>> cache.set('hello', 'world', seconds=5)
>>> # wait 6 seconds.
>>> cache.get('hello')

```

