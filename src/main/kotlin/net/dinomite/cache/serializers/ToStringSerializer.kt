package net.dinomite.cache.serializers

/**
 * Serialize objects using their toString() method.  Doesn't support deserialization—
 * this is just intended to make keys legible using the Redis console.
 */
class ToStringSerializer : ObjectStreamSerializer() {
    override fun serialize(obj: Any?): ByteArray = obj.toString().toByteArray()

    override fun <T> deserialize(objectData: ByteArray): T {
        throw UnsupportedOperationException("Deserialization isn't supported")
    }
}
