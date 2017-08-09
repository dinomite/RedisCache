package net.dinomite.cache.serializers

import com.google.common.primitives.Bytes
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.*

class ObjectStreamSerializerTest {
    private val serializer = ObjectStreamSerializer()

    @Test
    fun testSerialize() {
        val input = "foobar"
        val expected = Bytes.concat(byteArrayOf(-84, -19, 0, 5, 116, 0, 6), input.toByteArray())
        assertTrue(Arrays.equals(expected, serializer.serialize(input)))
    }

    @Test
    fun testDeserialize() {
        val expected = "foobar"
        val input = Bytes.concat(byteArrayOf(-84, -19, 0, 5, 116, 0, 6), expected.toByteArray())
        assertEquals(expected, serializer.deserialize(input))
    }

    @Test
    fun testRoundTrip() {
        val input = "foobar"
        assertEquals(input, serializer.deserialize(serializer.serialize(input)))
    }
}