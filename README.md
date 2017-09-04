# RedisCache

A [Guava LoadingCache](https://google.github.io/guava/releases/22.0/api/docs/com/google/common/cache/LoadingCache.html)
backed by Redis.

This project is written in [Kotlin](https://kotlinlang.org/) and the examples in this file also use
Kotlin syntax.  While different from Java, Kotlin's syntax doesn't stray too far so it should be
intelligible to those who only know Java.  Similarly, Kotlin's Java interop is great and I have
tried to write code that can be [easily used in Java](https://android.jlelse.eu/writing-java-friendly-kotlin-code-c408b24fb4e).
If you find shortcomings in that respect, let me know!

Additionally, I encourage anyone who uses Java to give Kotlin a try—it's better in every way.

# Dependency

Find the latest version from the [repository page on Bintray](https://bintray.com/dinomite/maven/net.dinomite%3Aredis-cache).

Use that version number to add the dependency to your `build.gradle`:

    compile 'net.dinomite:redis-cache:[latest_version]'

Or `pom.xml`, if you use Maven:

    <dependency>
      <groupId>net.dinomite</groupId>
      <artifactId>redis-cache</artifactId>
      <version>[latest_version]</version>
    </dependency>

# Usage

The only thing RedisCache needs is a [JedisPool](https://github.com/xetorthio/jedis/wiki/Getting-started#using-jedis-in-a-multithreaded-environment)
for interacting with Redis.  Thereafter, it acts just like an Guava LoadingCache, except values are
stored in Redis.[1]

    // Pretend that this is expensive
    fun generateValue(key: String): String { return "value-for-$key" }
    
    val jedisPool = JedisPool("localhost", 6379)
    val redisCache = RedisCache<String, String>(jedisPool)
    redisCache.put("foo", { generateValue(String) })

You can optionally pass a `String` prefix that will be prepended to all keys.  RedisCache does not
add any separator between the prefix and individual keys; provide your own in the prefix string if
desired.

    val redisCache = RedisCache<String, String>(jedisPool, prefix = "page-cache:")

By default RedisCache uses the prefix 'redis-cache:'.

RedisCache sets the TTL for values inserted into Redis to 1 hour by default—no other automatic
eviction is performed so **you probably want a different value**.  This is akin to `CacheBuilder`'s
[`expireAfterWrite`](https://github.com/google/guava/wiki/CachesExplained#timed-eviction) setting.
Pass the `expiration` parameter to set the TTL for all RedisCache-inserted objects.

## Expire after read

RedisCache can renew the TTL of values on each read if you set `expireAfterRead` to true:

    val redisCache = RedisCache<String, String>(jedisPool, expireAfterRead = true)
    // Every retrieval will update the value's TTL to the value of `expiration`

This mode is akin to `CacheBuilder`'s
[`expireAfterRead`](https://github.com/google/guava/wiki/CachesExplained#timed-eviction) setting.

## Custom serialization

By default, RedisCache serializes both values & keys with the JDK's [ObjectOutputStream](https://docs.oracle.com/javase/8/docs/api/java/io/ObjectOutputStream.html)
written as a byte array. This works for anything you give it, but the keys produced are inscrutible
(well, for strings it's teh object type preceding the string value):

    127.0.0.1:6379> keys *
    1) "\xac\xed\x00\x05t\x00\x06foobar"

You can provide RedisCache with custom serializers for keys & values as constructor arguments.  If
you want your keys to be liegibile in Redis, you can use ToStringSerializer, which uses the provided
key object's `.toString()` method for serialization.  You probably only want this if you are using
strings, or other objects with unique & representative `.toString()` values.

    val redisCache = RedisCache(jedisPool = jedisPool, keySerializer = ToStringSerializer())
    ...
    127.0.0.1:6379> keys *
    1) "foobar"

RedisCache sends all keys & values to Redis as byte arrays.  You can implement the `Serializer`
interface and pass your implementation for key and/or value serialization.

# Development

## Tests

Tests are performed against [embedded-redis](https://github.com/kstyrc/embedded-redis).
`RedisCacheTest` tests all methods for a single-argument (i.e. default serializers, no prefix
or expiration, no loader) constructed instance.  The other test classes do what they say on the
tin, testing custom serializers & a default loader.  I split those out because they require
constructing a different RedisCache instance, and only need to test a subset of methods.

# TODO

- Implement LRU eviction
- Smarter GetAll
- Transactions for multi-host support?
- SHA key serializer

# See also

RedisCache started as a port from [this Java implementation](https://github.com/levyfan/guava-cache-redis)

[1]: The intent is to allow the cached values to be shareable amongst multiple machines, but
I have yet to actually try that.  Likely some transactions are needed in the RedisCache code.
