package net.dinomite.cache

import net.dinomite.cache.serializers.Serializer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.util.*

class RedisCacheTest_WithSerializers {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>
    lateinit var serializer: Serializer

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

        redisCache = RedisCache(jedisPool, serializer, serializer)
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
}