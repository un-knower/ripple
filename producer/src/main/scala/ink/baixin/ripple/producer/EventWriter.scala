package ink.baixin.ripple.producer

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.models.Record
import io.netty.handler.codec.http.QueryStringDecoder

trait EventWriter {
  def putEvent(ts: Long, uri: String)
}

class KinesisEventWriter(region: String, stream: String, bufferTime: Long)
extends EventWriter {
  private val logger = Logger(this.getClass)
  private val config = new KinesisProducerConfiguration()
    .setRegion(region)
    .setRecordMaxBufferedTime(bufferTime)
  private val kinesis = new KinesisProducer(config)
  private val requiredKeys = Seq("aid", "cid", "sid")
  private val valueKeys = Seq(
    "ref", "scv", // entrance
    "et", "est", "epn", "eep", // events
    "unn", "uau", "ugd", "uct", "upn", "ucy", "ula", "uoid", // users
    "lgt", "lat", "lac" // location
  )

  private def getStringValue(map: java.util.Map[String, java.util.List[String]], key: String) = {
    if (map.containsKey(key)) {
      val list = map.get(key)
      if (list.size > 0) list.get(0) else ""
    } else ""
  }

  def putEvent(ts: Long, uri: String) {
    logger.debug(s"event=put_event stream=$stream timestamp=$ts uri=$uri")
    val decoder = new QueryStringDecoder(uri)
    if (decoder.path == "/w") {
      val parameters = decoder.parameters
      val pk = requiredKeys.map(k => getStringValue(parameters, k))
      if (pk.forall(v => !v.isEmpty)) {
        val values = valueKeys
          .map(k => (k, getStringValue(parameters, k)))
          .collect{
            case (k, v) if !v.isEmpty => (k, v)
          }
          .toMap
        try {
          val rec = Record(ts, pk(0).toInt, pk(1), pk(2), values)
          val key = s"${rec.appId}|${rec.clientId}|${rec.sessionId}"
          val data = ByteBuffer.wrap(rec.toByteArray)

          logger.debug(s"event=kinesis_add_user_record stream=$stream key=$key")
          kinesis.addUserRecord(stream, key, data)
        } catch {
          case e: Exception =>
          logger.error(s"event=failed_to_put_event error=$e")
        }
      }
    }
  }
}