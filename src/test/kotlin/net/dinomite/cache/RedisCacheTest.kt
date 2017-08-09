package net.dinomite.cache

import com.google.common.cache.CacheLoader
import com.google.common.primitives.Bytes
import net.dinomite.cache.serializers.ObjectStreamSerializer
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.time.Duration
import java.util.*

class RedisCacheTest {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>

    val prefix = "redis-cache:".toByteArray()
    val serializer = ObjectStreamSerializer()

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
    fun testGetIfPresent_NullForNonExistentKey() {
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
            assertEquals("Loader must not return null, key=foobar", e.message)
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
    fun testPut_UsesPrefix() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        jedisPool.resource.use { jedis ->
            val serializedKey = Bytes.concat(prefix, serializer.serialize(key))
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
            assertEquals(Duration.ofHours(1).seconds, jedis.ttl(redisCache.buildKey(key)))
        }
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
    fun testBuildKey_IncludesPrefix() {
        val key = "foobar"
        val expected = Bytes.concat(prefix, serializer.serialize(key))
        val actual = redisCache.buildKey(key)
        assertTrue(Arrays.equals(expected, actual))
    }
}