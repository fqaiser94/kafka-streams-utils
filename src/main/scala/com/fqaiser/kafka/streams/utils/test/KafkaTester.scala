package com.fqaiser.kafka.streams.utils.test

import org.apache.kafka.streams.Topology
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers

trait KafkaTester extends Matchers {

  trait Topic {
    val name: String
    val numPartitions: Int
    def create(): Unit
  }

  trait InputTopic[K, V] extends Topic {
    def pipeInput(key: K, value: V, partition: Integer = null): Unit
  }

  case class Record[K, V](key: K, value: V, partition: Integer = null)

  trait OutputTopic[K, V] extends Topic {
    def readKeyValuesToList(): List[Record[K, V]]
  }

  trait Stream {
    def start(): Unit
    def close(): Unit
  }

  val topology: Topology
  def buildStream(topology: Topology): Stream

  case class InputOutputTopics(input: Seq[Topic], output: Seq[Topic])

  final type TestFunction = InputOutputTopics => Assertion

  def inputOutputTopics(): InputOutputTopics

  def runKafkaTest(testFunction: TestFunction): Unit = {
    val stream = buildStream(topology)
    try {
      val params = inputOutputTopics()
      params.input.foreach(_.create())
      params.output.foreach(_.create())
      stream.start()
      testFunction(params)
    } finally {
      stream.close()
    }
  }

  def topicShouldContainTheSameElementsInOrderAs[K, V](
      outputTopic: OutputTopic[K, V],
      expected: Seq[Record[K, V]]
  ): Assertion = {
    val result = outputTopic.readKeyValuesToList()
    result should contain theSameElementsInOrderAs expected
  }
}
