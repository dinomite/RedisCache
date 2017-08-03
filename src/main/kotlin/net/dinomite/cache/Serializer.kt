package net.dinomite.cache

interface Serializer {
    fun serialize(obj: Any?): ByteArray

    fun <T> deserialize(objectData: ByteArray): T
}
