incache
---------------

Simple caching decorator that allows you to use redis like a cache.

    from ncache import cachable

    @cachable(seconds=500)
    def sum(a,b):
        return a+b

    sum(4,5) # an expensive calculation indeed :-)

    9 is cached

By default the function name and arguments are concatonated to create the key. All values are pickled before adding to the cache.

    @cachable()
sets a key permanently

    @cachable(seconds=300)
caches for 300 seconds

    @cachable(key_name='mykey')
sets a specific key_name

    @cachable(pickled=True, hash_keys=False)
Will pickle values before storing them but no hash the key name.  This is the default behaviour

    @cachable(prefix="STORE1")
Will prefix all keys with the plain text STORE1. This is handy for group selecting ranges of keys on the redis-cli.  Prefixes are never hashed for this reason so you can do your dev work.

    @cachable()
        def sum(a, b):
        return a + b

By default the key name is the function name _ *args _ **kw.  So sum(3, 4) is caches as key "sum_(3, 4)_{}"

