package net.dinomite.cache

import com.google.common.cache.AbstractLoadingCache
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Iterables
import com.google.common.primitives.Bytes
import redis.clients.jedis.JedisPool
import sun.plugin.dom.exception.InvalidStateException
import java.time.Duration
import java.util.*
import java.util.concurrent.Callable

class RedisCache<K, V>(val jedisPool: JedisPool,
                       val keySerializer: Serializer = ObjectStreamSerializer(),
                       val valueSerializer: Serializer = ObjectStreamSerializer(),
                       val keyPrefix: ByteArray? = null,
                       expiration: Duration? = null,
                       val loader: CacheLoader<K, V>? = null)
    : AbstractLoadingCache<K, V>(), LoadingCache<K, V> {

    val expiration: Int? = expiration?.seconds?.toInt()

    override fun getIfPresent(key: Any): V? {
        jedisPool.resource.use { jedis ->
            val reply = jedis.get(buildKey(key))
            if (reply == null) {
                return null
            } else {
                return valueSerializer.deserialize(reply)
            }
        }
    }

    @Throws(CacheLoader.InvalidCacheLoadException::class)
    override fun get(key: K, valueLoader: Callable<out V>): V {
        var value = this.getIfPresent(key as Any)
        if (value == null) {
            value = valueLoader.call()
            if (value == null) {
                throw CacheLoader.InvalidCacheLoadException("Loader must not return null, key=" + key)
            } else {
                this.put(key, value)
            }
        }

        return value
    }

    @Suppress("UNCHECKED_CAST")
    override fun getAllPresent(keys: Iterable<Any?>): ImmutableMap<K, V> {
        val keyBytes = keys.map { buildKey(it) }

        jedisPool.resource.use { jedis ->
            val valueBytes = jedis.mget(*Iterables.toArray(keyBytes, ByteArray::class.java))

            val result = LinkedHashMap<K, V>()
            keys.map { it as K }.forEachIndexed { i, castKey ->
                if (valueBytes[i] != null) {
                    result.put(castKey, valueSerializer.deserialize<V>(valueBytes[i]))
                }
            }
            return ImmutableMap.copyOf(result)
        }
    }

    override fun getAll(keys: MutableIterable<K>?): ImmutableMap<K, V> {
        // TODO Do getAllPresent(), then use pipeline to load remaining values
        return super.getAll(keys)
    }

    override fun put(key: K?, value: V?) {
        val keyBytes = buildKey(key)
        val valueBytes = valueSerializer.serialize(value)

        jedisPool.resource.use { jedis ->
            if (expiration != null) {
                jedis.setex(keyBytes, expiration, valueBytes)
            } else {
                jedis.set(keyBytes, valueBytes)
            }
        }
    }

    @Suppress("IMPLICIT_CAST_TO_ANY")
    override fun putAll(m: Map<out K, V>) {
        val keysAndValues = ArrayList<ByteArray>()
        m.forEach { key, value ->
            keysAndValues.add(buildKey(key))
            keysAndValues.add(valueSerializer.serialize(value))
        }

        jedisPool.resource.use { jedis ->
            if (expiration != null) {
                jedis.pipelined().use { pipeline ->
                    pipeline.mset(*Iterables.toArray(keysAndValues, ByteArray::class.java))
                    var i = 0
                    while (i < keysAndValues.size) {
                        pipeline.expire(keysAndValues[i], expiration)
                        i += 2
                    }

                    pipeline.exec()
                }
            } else {
                jedis.mset(*Iterables.toArray(keysAndValues, ByteArray::class.java))
            }
        }
    }

    override fun invalidate(key: Any?) {
        jedisPool.resource.use { jedis ->
            jedis.del(buildKey(key))
        }
    }

    override fun invalidateAll(keys: Iterable<*>) {
        val keyBytes = keys.map { buildKey(it) }

        jedisPool.resource.use { jedis ->
            jedis.del(*Iterables.toArray(keyBytes, ByteArray::class.java))
        }
    }

    override fun get(key: K): V {
        if (loader == null) {
            throw InvalidStateException("Cannot use single-argument get with null loader (provide one Cache construction)")
        }

        return this.get(key, { loader.load(key) })
    }

    override fun refresh(key: K) {
        if (loader == null) {
            throw InvalidStateException("Cannot refresh wih null loader (provide one Cache construction)")
        }

        this.put(key, loader.load(key))
    }

    internal fun buildKey(key: Any?): ByteArray {
        if (keyPrefix == null) {
            return keySerializer.serialize(key)
        } else {
            return Bytes.concat(keyPrefix, keySerializer.serialize(key))
        }
    }
}