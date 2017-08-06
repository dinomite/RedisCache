package net.dinomite.cache

import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.AbstractLoadingCache
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Iterables
import com.google.common.primitives.Bytes
import net.dinomite.cache.serializers.ObjectStreamSerializer
import net.dinomite.cache.serializers.Serializer
import redis.clients.jedis.JedisPool
import java.time.Duration
import java.util.*
import java.util.concurrent.Callable

/**
 * A Redis-backed [LoadingCache](https://google.github.io/guava/releases/22.0/api/docs/com/google/common/cache/LoadingCache.html)
 *
 *     val jedisPool = JedisPool("localhost", 6379)
 *     val redisCache = RedisCache<String, String>(jedisPool)
 *     redisCache.put("foo", { generateValue(String) })
 */
class RedisCache<K, V>
@JvmOverloads constructor(val jedisPool: JedisPool,
                          val keySerializer: Serializer = ObjectStreamSerializer(),
                          val valueSerializer: Serializer = ObjectStreamSerializer(),
                          val keyPrefix: ByteArray = "redis-cache:".toByteArray(),
                          expiration: Duration = Duration.ofHours(1),
                          val loader: CacheLoader<K, V>? = null)
    : AbstractLoadingCache<K, V>(), LoadingCache<K, V> {

    val expiration: Int? = expiration.seconds.toInt()

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

    @Suppress("UNCHECKED_CAST", "Handles generic objects")
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

    @Suppress("IMPLICIT_CAST_TO_ANY", "Handles generic objects")
    override fun putAll(items: Map<out K, V>) {
        val keysAndValues = ArrayList<ByteArray>()
        items.forEach { key, value ->
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

                    pipeline.sync()
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
            throw IllegalStateException("Cannot use single-argument get with null loader (provide one Cache construction)")
        }

        return this.get(key, { loader.load(key) })
    }

    override fun refresh(key: K) {
        if (loader == null) {
            throw IllegalStateException("Cannot refresh wih null loader (provide one Cache construction)")
        }

        this.put(key, loader.load(key))
    }

    @VisibleForTesting
    fun buildKey(key: Any?): ByteArray {
        return Bytes.concat(keyPrefix, keySerializer.serialize(key))
    }
}