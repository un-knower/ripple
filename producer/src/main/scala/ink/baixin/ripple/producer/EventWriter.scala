package ink.baixin.ripple.producer

import com.typesafe.scalalogging.Logger
import io.netty.handler.codec.http.QueryStringDecoder
import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import ink.baixin.ripple.core.models.Record

trait EventWriter {
  def putEvent(ts: Long, uri: String)
}

class KinesisEventWriter(val region:String, val bufferTime: Long) extends EventWriter {

  private val logger = Logger(this.getClass)
  private val config = new KinesisProducerConfiguration()
    .setRegion(region)
    .setRecordMaxBufferedTime(bufferTime)

  private val kinesis = new KinesisProducer(config)

  private val productionNamespaces = Set(
    "/w", "/wpre"
  )

  private val testNamespaces = Set(
    "/wuat", "/wuat2", "/wuat3", "/wstaging", "/wqa"
  )
  private val namespaces = productionNamespaces ++ testNamespaces

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
      if (list.size > 0) list.get(0)
      else ""
    } else ""
  }

  private def getStream(ns: String) = {
    if (productionNamespaces.contains(ns)) "wmp-analytics"
    else "wmp-analytics-qa"
  }

  def putEvent(ts: Long, uri: String) {
    logger.debug(s"event=put_event timestamp=$ts uri=$uri")
    val decoder = new QueryStringDecoder(uri)

    if (namespaces.contains(decoder.path)) {
      val parameters = decoder.parameters
      val pk = requiredKeys.map(k => getStringValue(parameters, k))
      if (pk.forall(v => !v.isEmpty)) {
        val values = valueKeys
          .map{ k => (k, getStringValue(parameters, k)) }
          .collect { case (k, v) if !v.isEmpty => (k, v) }
          .toMap

        try {
          val stream = getStream(decoder.path)
          val rec = Record(ts, pk(0).toInt, pk(1), pk(2), 0, decoder.path, values)
          val key = s"${rec.appId}|${rec.clientId}|${rec.sessionId}"
          val data = java.nio.ByteBuffer.wrap(rec.toByteArray)

          logger.debug(s"event=kinesis_add_user_record stream=$stream namespace=${decoder.path} key=$key")
          kinesis.addUserRecord(stream, key, data)
        } catch {
          case e: Exception =>
            logger.error(s"event=failed_to_put_event error=$e")
        }
      }
    }
  }
}