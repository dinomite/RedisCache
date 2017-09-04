package net.dinomite.cache

import net.dinomite.cache.serializers.ToStringSerializer
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer

class RedisCacheTest_WithDatabase {
    private val port = 16379
    private val redisServer: RedisServer = RedisServer(port)
    private val jedisPool = JedisPool("localhost", port)
    private val database = 7

    lateinit var redisCache: RedisCache<String, String>

    @Before
    fun setup() {
        redisServer.start()

        redisCache = RedisCache(jedisPool, database = 7, keySerializer = ToStringSerializer(), keyPrefix = "".toByteArray())
    }

    @After
    fun cleanup() {
        jedisPool.resource.use { it.flushDB() }
    }

    @Test
    fun testPut() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)
        assertEquals(value, redisCache.getIfPresent(key))

        jedisPool.resource.use {
            assertNull("Should not exist in default DB", it.get(key))
            it.select(database)
            assertNotNull("Should exist in selected DB", it.get(key))
        }
    }

    @Test
    fun testInvalidateAll_FlushesOwnDBOnly() {
        val specialKey = "specialKey"
        val specialValue = "specialValue"
        jedisPool.resource.use {
            it.set(specialKey, specialValue)
        }

        val key = "foo"
        redisCache.put(key, "bar")
        redisCache.invalidateAll()
        assertNull(redisCache.getIfPresent(key))

        jedisPool.resource.use {
            assertEquals(specialValue, it.get(specialKey))
        }
    }
}