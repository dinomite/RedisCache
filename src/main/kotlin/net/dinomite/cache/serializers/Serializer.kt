package net.dinomite.cache.serializers

interface Serializer {
    fun serialize(obj: Any?): ByteArray

    fun <T> deserialize(objectData: ByteArray): T
}
