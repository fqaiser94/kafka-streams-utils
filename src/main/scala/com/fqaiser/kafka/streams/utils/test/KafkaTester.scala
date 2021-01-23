package com.fqaiser.kafka.streams.utils.test

import org.apache.kafka.streams.{KeyValue, Topology}
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers

trait KafkaTester extends Matchers {

  trait Topic {
    val name: String
  }

  trait InputTopic[K, V] extends Topic {
    def pipeInput(key: K, value: V): Unit
  }

  trait OutputTopic[K, V] extends Topic {
    def readKeyValuesToList(): List[KeyValue[K, V]]
  }

  trait Stream {
    def start(): Unit
    def close(): Unit
  }

  val topology: Topology
  def buildStream(topology: Topology): Stream

  type TestFunctionEnvironment

  final type TestFunction = TestFunctionEnvironment => Assertion

  def testFunctionParams: TestFunctionEnvironment

  def runKafkaTest(testFunction: TestFunction): Unit = {
    val stream = buildStream(topology)
    try {
      stream.start()
      testFunction(testFunctionParams)
    } finally {
      stream.close()
    }
  }

  def topicShouldContainTheSameElementsAs[K, V](
      outputTopic: OutputTopic[K, V],
      expected: Seq[KeyValue[K, V]]
  ): Assertion = {
    val result = outputTopic.readKeyValuesToList()
    result should contain theSameElementsInOrderAs expected
  }
}
