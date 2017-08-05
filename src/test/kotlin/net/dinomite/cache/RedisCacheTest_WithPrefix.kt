package net.dinomite.cache

import com.google.common.primitives.Bytes
import net.dinomite.cache.serializers.ObjectStreamSerializer
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.util.*

class RedisCacheTest_WithPrefix {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>

    val prefix = "prefix-"
    val serializer = ObjectStreamSerializer()

    @Before
    fun setup() {
        redisServer.start()

        redisCache = RedisCache(jedisPool, keyPrefix = prefix.toByteArray())
    }

    @After
    fun cleanup() {
        jedisPool.resource.use { it.flushDB() }
    }

    @Test
    fun testPut_UsesPrefix() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        jedisPool.resource.use { jedis ->
            val serializedKey = Bytes.concat(prefix.toByteArray(), serializer.serialize(key))
            val serializedValue = serializer.serialize(value)
            assertTrue(Arrays.equals(serializedValue, jedis.get(serializedKey)))
        }
    }

    @Test
    fun testBuildKey_IncludesPrefix() {
        val key = "foobar"
        val expected = Bytes.concat(prefix.toByteArray(), serializer.serialize(key))
        val actual = redisCache.buildKey(key)
        assertTrue(Arrays.equals(expected, actual))
    }
}