package net.dinomite.cache

import com.google.common.cache.CacheLoader
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer

class RedisCacheTest_WithLoader {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>

    @Before
    fun setup() {
        redisServer.start()

        redisCache = RedisCache(jedisPool, loader = object: CacheLoader<String, String>() {
            override fun load(key: String): String {
                return "value-for-$key"
            }
        })
    }

    @After
    fun cleanup() {
        jedisPool.resource.use { it.flushDB() }
    }

    @Test
    fun testGetIfPresent_NullForNonexistantKey() {
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
    fun testGet_WithLoader_InsertsCustomValue() {
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
    fun testGet_LoadsValueWithLoader() {
        assertEquals("value-for-foo", redisCache.get("foo"))
    }

    @Test
    fun testRefresh_LoadsValueWithLoader() {
        redisCache.put("foo", "custom value")
        redisCache.refresh("foo")
        assertEquals("value-for-foo", redisCache.get("foo"))
    }
}