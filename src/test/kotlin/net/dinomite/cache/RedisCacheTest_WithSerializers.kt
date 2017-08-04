package net.dinomite.cache

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.time.Duration
import java.util.*

class RedisCacheTest_WithSerializers {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>
    lateinit var serializer: Serializer

    val prefix = "prefix-"
    val expiration: Duration = Duration.ofMinutes(7)

    @Before
    fun setup() {
        redisServer.start()

        serializer = object: Serializer {
            override fun serialize(obj: Any?): ByteArray {
                with(obj as String) {
                    return toByteArray()
                }
            }

            @Suppress("UNCHECKED_CAST")
            override fun <T> deserialize(objectData: ByteArray): T {
                return String(objectData) as T
            }
        }

        redisCache = RedisCache(jedisPool, serializer, serializer, prefix.toByteArray(), expiration)
    }

    @After
    fun cleanup() {
        jedisPool.resource.use { it.flushDB() }
    }

    @Test
    fun testGet() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        assertEquals(value, redisCache.get(key, { value }))
    }

    @Test
    fun testPut_UsesPrefix() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        jedisPool.resource.use { jedis ->
            val serializedKey = serializer.serialize(prefix + key)
            val serializedValue = serializer.serialize(value)
            assertTrue(Arrays.equals(serializedValue, jedis.get(serializedKey)))
        }
    }

    @Test
    fun testPut_SetsExpiration() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        jedisPool.resource.use { jedis ->
            val serializedKey = serializer.serialize(prefix + key)
            assertEquals(expiration.seconds, jedis.ttl(serializedKey))
        }
    }

    @Test
    fun testBuildKey_IncludesPrefix() {
        val key = "foobar"
        val expected = serializer.serialize(prefix) + key.toByteArray()
        val actual = redisCache.buildKey(key)
        assertTrue(Arrays.equals(expected, actual))
    }
}