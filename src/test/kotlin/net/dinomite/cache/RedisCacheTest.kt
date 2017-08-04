package net.dinomite.cache

import com.google.common.cache.CacheLoader
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.util.*

class RedisCacheTest {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>

    @Before
    fun setup() {
        redisServer.start()

        redisCache = RedisCache(jedisPool)
    }

    @After
    fun cleanup() {
        jedisPool.resource.use { it.flushDB() }
    }

    @Test
    fun testGetIfPresent_NullForNonExistantKey() {
        val key = "foobar"
        assertNull(redisCache.getIfPresent(key))
    }

    @Test
    fun testGetIfPresent_ReturnsValueThatExists() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)
        assertEquals(value, redisCache.getIfPresent(key))
    }

    @Test
    fun testGet_WithLoader_LoadsValue() {
        val key = "foobar"
        val value = "foovalue"
        assertEquals(value, redisCache.get(key, { value }))
    }

    @Test
    fun testGet_WithLoader_ReturnsExistingValue() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)
        assertEquals(value, redisCache.get(key, { value + "foobar" }))
    }

    @Test
    fun testGet_WithLoader_ExceptionForNullFromValueLoader() {
        val key = "foobar"
        try {
            redisCache.get(key, { null })
            fail("Expected CacheLoader.InvalidCacheLoadException")
        } catch (e: CacheLoader.InvalidCacheLoadException) {
            // Expected
        }
    }

    @Test
    fun testGetAllPresent_GetsAll() {
        val firstKey = "foo"
        val firstValue = "bar"
        val secondKey = "baz"
        val secondValue = "qux"
        val expected = mapOf(firstKey to firstValue, secondKey to secondValue)
        redisCache.putAll(expected)

        assertEquals(expected, redisCache.getAllPresent(listOf(firstKey, secondKey)))
    }

    @Test
    fun testGetAllPresent_SkipsMissing() {
        val firstKey = "foo"
        val firstValue = "bar"
        val secondKey = "baz"
        val secondValue = "qux"
        val expected = mapOf(firstKey to firstValue, secondKey to secondValue)
        redisCache.putAll(expected)

        assertEquals(expected, redisCache.getAllPresent(listOf(firstKey, secondKey, "quxx")))
    }

    @Test
    fun testPut() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)
        assertEquals(value, redisCache.getIfPresent(key))
    }

    @Test
    fun testPutAll() {
        val firstKey = "foo"
        val firstValue = "bar"
        val secondKey = "baz"
        val secondValue = "qux"
        val expected = mapOf(firstKey to firstValue, secondKey to secondValue)

        redisCache.putAll(expected)

        assertEquals(expected, redisCache.getAllPresent(listOf(firstKey, secondKey)))
    }

    @Test
    fun testInvalidate() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        redisCache.invalidate(key)

        assertNull(redisCache.getIfPresent(key))
    }

    @Test
    fun testInvalidateAll() {
        val firstKey = "foo"
        val firstValue = "bar"
        val secondKey = "baz"
        val secondValue = "qux"
        val expected = mapOf(firstKey to firstValue, secondKey to secondValue)
        redisCache.putAll(expected)
        val keys = listOf(firstKey, secondKey)

        redisCache.invalidateAll(keys)

        assertTrue(redisCache.getAllPresent(keys).isEmpty())
    }

    @Test
    fun testGet_ThrowsExceptionForMissingLoader() {
        try {
            redisCache.get("foobar")
            fail("Expected IllegalStateException")
        } catch (e: IllegalStateException) {
            // Expected
        }
    }

    @Test
    fun testRefresh_ThrowsExceptionForMissingLoader() {
        try {
            redisCache.refresh("foobar")
            fail("Expected IllegalStateException")
        } catch (e: IllegalStateException) {
            // Expected
        }
    }

    @Test
    fun testBuildKey() {
        val expected = byteArrayOf(-84, -19, 0, 5, 116, 0, 6, 102, 111, 111, 98, 97, 114)
        val actual = redisCache.buildKey("foobar")
        assertTrue(Arrays.equals(expected, actual))
    }
}