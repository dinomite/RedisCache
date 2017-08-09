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
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import java.time.Duration
import java.util.*
import java.util.concurrent.Callable

/**
 * A Redis-backed [LoadingCache](https://google.github.io/guava/releases/22.0/api/docs/com/google/common/cache/LoadingCache.html)
 *
 *     val jedisPool = JedisPool("localhost", 6379)
 *     val redisCache = RedisCache<String, String>(jedisPool)
 *     redisCache.put("foo", { generateValue(String) })
 *
 * All keys are inserted with a expiration (1 hour unless specified) and prefix (the byte array representation of
 * "redis-cache:").  Setting `expireAfterRead` will refresh each key's expiration upon read.
 */
class RedisCache<K, V>
@JvmOverloads constructor(private val jedisPool: JedisPool,
                          private val keySerializer: Serializer = ObjectStreamSerializer(),
                          private val valueSerializer: Serializer = ObjectStreamSerializer(),
                          private val keyPrefix: ByteArray = "redis-cache:".toByteArray(),
                          expiration: Duration = Duration.ofHours(1),
                          private val loader: CacheLoader<K, V>? = null,
                          private val expireAfterRead: Boolean = false)
    : AbstractLoadingCache<K, V>(), LoadingCache<K, V> {

    private val expiration: Int = expiration.seconds.toInt()

    override fun getIfPresent(key: Any): V? {
        var ret: V? = null

        pipeline { p ->
            val locator = buildKey(key)
            val reply = p.get(locator)
            if (expireAfterRead) p.expire(locator, expiration)
            p.sync()

            val result = reply.get()
            if (result != null) {
                ret = valueSerializer.deserialize(result)
            }
        }

        return ret
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
        var ret: Map<K, V> = mapOf()

        pipeline { p ->
            val locators = keys.map { buildKey(it) }
            val valueBytes = p.mget(*Iterables.toArray(locators, ByteArray::class.java))
            p.sync()

            val result = LinkedHashMap<K, V>()
            keys.map { it as K }.forEachIndexed { i, castKey ->
                val value = valueBytes.get()[i]
                if (value != null) {
                    if (expireAfterRead) p.expire(locators[i], expiration)
                    result.put(castKey, valueSerializer.deserialize(value))
                }
            }

            ret = result
        }

        return ImmutableMap.copyOf(ret)
    }

    override fun getAll(keys: MutableIterable<K>?): ImmutableMap<K, V> {
        // TODO Do getAllPresent(), then use pipeline to load remaining values
        return super.getAll(keys)
    }

    override fun put(key: K?, value: V?) {
        val keyBytes = buildKey(key)
        val valueBytes = valueSerializer.serialize(value)

        jedis { jedis ->
            jedis.setex(keyBytes, expiration, valueBytes)
        }
    }

    @Suppress("IMPLICIT_CAST_TO_ANY", "Handles generic objects")
    override fun putAll(items: Map<out K, V>) {
        val keysAndValues = ArrayList<ByteArray>()
        items.forEach { key, value ->
            keysAndValues.add(buildKey(key))
            keysAndValues.add(valueSerializer.serialize(value))
        }

        pipeline { p ->
            p.mset(*Iterables.toArray(keysAndValues, ByteArray::class.java))

            var i = 0
            while (i < keysAndValues.size) {
                p.expire(keysAndValues[i], expiration)
                i += 2
            }
            p.sync()
        }
    }

    override fun invalidate(key: Any?) {
        jedis { jedis ->
            jedis.del(buildKey(key))
        }
    }

    override fun invalidateAll(keys: Iterable<*>) {
        val keyBytes = keys.map { buildKey(it) }

        jedis { jedis ->
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

    /**
     * Serialize the given key & add the prefix
     */
    @VisibleForTesting
    internal fun buildKey(key: Any?): ByteArray {
        checkNotNull(key) { "Key cannot be null" }
        return Bytes.concat(keyPrefix, keySerializer.serialize(key))
    }

    private fun <T> jedis(body: (jedis: Jedis) -> T): T = jedisPool.resource.use { body(it) }

    private fun pipeline(body: (pipeline: Pipeline) -> Unit) {
        jedisPool.resource.use {
            it.pipelined().use {
                body(it)
                it.sync()
            }
        }
    }
}