package net.dinomite.cache

import net.dinomite.cache.serializers.ObjectStreamSerializer
import net.dinomite.cache.serializers.Serializer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.time.Duration

class RedisCacheTest_WithExpiration {
    val port = 16379
    val redisServer: RedisServer = RedisServer(port)
    val jedisPool = JedisPool("localhost", port)

    lateinit var redisCache: RedisCache<String, String>

    val expiration: Duration = Duration.ofMinutes(7)
    val serializer: Serializer = ObjectStreamSerializer()

    @Before
    fun setup() {
        redisServer.start()

        redisCache = RedisCache(jedisPool, expiration = expiration)
    }

    @After
    fun cleanup() {
        jedisPool.resource.use { it.flushDB() }
    }

    @Test
    fun testPut_SetsExpiration() {
        val key = "foobar"
        val value = "foovalue"
        redisCache.put(key, value)

        jedisPool.resource.use { jedis ->
            val serializedKey = serializer.serialize(key)
            assertEquals(expiration.seconds, jedis.ttl(serializedKey))
        }
    }
}