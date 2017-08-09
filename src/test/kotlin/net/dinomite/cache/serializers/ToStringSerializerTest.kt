package net.dinomite.cache.serializers

import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import java.util.*

class ToStringSerializerTest {
    private val serializer = ToStringSerializer()

    @Test
    fun testSerialize() {
        val input = "foobar"
        Assert.assertTrue(Arrays.equals(input.toByteArray(), serializer.serialize(input)))
    }

    @Test
    fun testDeserialize() {
        try {
            serializer.deserialize<String>(byteArrayOf(7))
            fail("Expected exception")
        } catch (e: UnsupportedOperationException) {
            assertEquals("Deserialization isn't supported", e.message)
        }
    }
}