package net.dinomite.cache.serializers

import java.io.*

/**
 * Serialize & deserialize objects to byte streams using the JDK's
 * ObjectOutputStream and ObjectInputStream.
 */
open class ObjectStreamSerializer : Serializer {
    override fun serialize(obj: Any?): ByteArray {
        val baos = ByteArrayOutputStream(512)
        try {
            ObjectOutputStream(baos).use { out -> out.writeObject(obj as Serializable) }
        } catch (ex: IOException) {
            throw SerializationException(ex)
        }
        return baos.toByteArray()
    }

    @Suppress("UNCHECKED_CAST", "Gotta happen ¯\\_(ツ)_/¯ ")
    override fun <T> deserialize(objectData: ByteArray): T {
        try {
            ObjectInputStream(ByteArrayInputStream(objectData)).use {
                return it.readObject() as T
            }
        } catch (ex: ClassNotFoundException) {
            throw SerializationException(ex)
        } catch (ex: IOException) {
            throw SerializationException(ex)
        }
    }
}
