package net.dinomite.cache.serializers

/**
 * Serialize objects using their toString() method
 */
class ToStringSerializer : ObjectStreamSerializer() {
    override fun serialize(obj: Any?): ByteArray {
        return obj.toString().toByteArray()
    }

    override fun <T> deserialize(objectData: ByteArray): T {
        throw UnsupportedOperationException("Deserialization isn't supported")
    }
}
