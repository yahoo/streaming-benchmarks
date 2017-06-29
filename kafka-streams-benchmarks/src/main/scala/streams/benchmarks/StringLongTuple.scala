package streams.benchmarks

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.kafka.common.errors.SerializationException

import scala.language.implicitConversions

case class StringLongTuple(str: String, l: Long)

class StringLongSerializer extends Serializer[StringLongTuple] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(s: String, t: StringLongTuple): Array[Byte] = {
    if (t == null) null
    else ScalaKryoInstantiator.defaultPool.toBytesWithClass(t)
  }

  override def close(): Unit = {}
}


class StringLongDeserializer extends Deserializer[StringLongTuple] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def deserialize(s: String, bytes: Array[Byte]): StringLongTuple = {
    if (bytes == null) null
    else if (bytes.isEmpty) throw new SerializationException("byte array must not be empty")
    else ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[StringLongTuple]
  }

  override def close(): Unit = {}
}

object StringLongSerde {
  val serde: Serde[StringLongTuple] = Serdes.serdeFrom(new StringLongSerializer, new StringLongDeserializer)
  implicit def tuple2stringLong(t: (String, Long)): StringLongTuple = StringLongTuple(t._1, t._2)
  implicit def stringLong2tuple(t: StringLongTuple): (String, Long) = (t.str, t.l)
}
