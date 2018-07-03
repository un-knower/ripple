package ink.baixin.ripple.producer

import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.typesafe.scalalogging.Logger
import io.netty.handler.codec.http.QueryStringDecoder

trait EventWriter {
  def putEvents(ts: Long, uri: String)
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
  private val doubleKeys = Seq()

  private def getStringValue(map: Map[String, List[String]], key: String) = {
    if (map.contains(key)) {
      val list = map.get(key)
      if (list.size > 0) list.get(0) else ""
    } else ""
  }

  def putEvents(ts: Long, uri: String) {
    logger.debug(s"event=put_event stream=$stream timestamp=$ts uri=$uri")
    val decoder = new QueryStringDecoder(uri)
  }

}
