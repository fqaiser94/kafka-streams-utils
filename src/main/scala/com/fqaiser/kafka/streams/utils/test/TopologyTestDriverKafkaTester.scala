package com.fqaiser.kafka.streams.utils.test

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}
import org.scalatest.compatible.Assertion

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

  /**
    * Does not check partition values are the same.
    */
  override final def topicShouldContainTheSameElementsInOrderAs[K, V](
      outputTopic: OutputTopic[K, V],
      expected: Seq[Record[K, V]]
  ): Assertion = {
    super.topicShouldContainTheSameElementsInOrderAs(outputTopic, expected.map(_.copy(partition = null)))
  }

  final case class MockInputTopic[K, V](name: String, numPartitions: Int = 1)(implicit
      keySer: Serializer[K],
      valSer: Serializer[V]
  ) extends InputTopic[K, V] {
    var topic: TestInputTopic[K, V] = _
    override def create(): Unit = topic = testDriver.createInputTopic(name, keySer, valSer)
    override def pipeInput(key: K, value: V, partition: Integer = null): Unit = topic.pipeInput(key, value)
  }

  final case class MockOutputTopic[K, V](name: String, numPartitions: Int = 1)(implicit
      keyDes: Deserializer[K],
      valDes: Deserializer[V]
  ) extends OutputTopic[K, V] {
    var topic: TestOutputTopic[K, V] = _
    override def create(): Unit = topic = testDriver.createOutputTopic(name, keyDes, valDes)
    override def readKeyValuesToList(): List[Record[K, V]] =
      topic.readRecordsToList().asScala.toList.map(x => Record(x.key, x.value, null))
  }

}
