package com.fqaiser.kafka.streams.utils.test

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, Topology, TopologyTestDriver}

import java.util.Properties
import scala.jdk.CollectionConverters.asScalaBufferConverter

trait TopologyTestDriverKafkaTester extends KafkaTester {

  var testDriver: TopologyTestDriver = _

  override def buildStream(topology: Topology): Stream = {
    val streamsConfig: Properties = {
      val props = new Properties()
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
      props
    }
    testDriver = new TopologyTestDriver(topology, streamsConfig)
    new Stream {
      override def start(): Unit = Unit

      override def close(): Unit = testDriver.close()
    }
  }

  final case class MockInputTopic[K, V](name: String)(implicit keySer: Serializer[K], valSer: Serializer[V])
      extends InputTopic[K, V] {
    override def pipeInput(key: K, value: V): Unit =
      testDriver.createInputTopic(name, keySer, valSer).pipeInput(key, value)
  }

  final case class MockOutputTopic[K, V](name: String)(implicit keyDes: Deserializer[K], valDes: Deserializer[V])
      extends OutputTopic[K, V] {
    override def readKeyValuesToList(): List[KeyValue[K, V]] =
      testDriver
        .createOutputTopic(name, keyDes, valDes)
        .readKeyValuesToList()
        .asScala
        .toList
  }

}
