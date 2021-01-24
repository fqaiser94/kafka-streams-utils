package com.fqaiser.kafka.streams.utils.test

import io.github.azhur.kafkaserdeavro4s.Avro4sBinarySupport
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

object RepartitionCorrectly {

  object App extends Avro4sBinarySupport {
    def genTopology(inputTopicName: String, outputTopicName: String): Topology = {
      val builder = new StreamsBuilder()

      builder
        .stream[InputKey, InputValue](inputTopicName)
        .repartition
        .to(outputTopicName)

      builder.build()
    }
  }

  trait Tests extends AnyFeatureSpec with Matchers {
    self: AppTester with KafkaTester =>

    Feature("Basic App") {
      Scenario("Adds 1 to both key and value") {
        runKafkaTest {
          case InputOutputTopics(Seq(inputTopic: TestInputTopic), Seq(outputTopic: TestOutputTopic)) =>
            // same key being sent to 2 different partitions
            // TODO: take Record as input
            inputTopic.pipeInput(InputKey(), InputValue(1), 0)
            inputTopic.pipeInput(InputKey(), InputValue(1), 1)
            // should end up on same partition
            topicShouldContainTheSameElementsInOrderAs(
              outputTopic,
              Seq(Record(OutputKey(), OutputValue(1), 0), Record(OutputKey(), OutputValue(1), 0))
            )

        }
      }
    }
  }

  trait AppTester extends Avro4sBinarySupport {
    self: KafkaTester =>

    val inputTopicName = "InputTopic"
    val outputTopicName = "OutputTopic"

    type TestInputTopic = InputTopic[InputKey, InputValue]
    type TestOutputTopic = OutputTopic[OutputKey, OutputValue]

    def makeTestInputTopic(name: String): TestInputTopic
    def makeTestOutputTopic(name: String): TestOutputTopic
    override def inputOutputTopics() =
      InputOutputTopics(makeTestInputTopic(inputTopicName) :: Nil, makeTestOutputTopic(outputTopicName) :: Nil)

    override lazy val topology: Topology = App.genTopology(inputTopicName, outputTopicName)
  }
}

class RepartitionCorrectlyAppTestsWithTopologyTestDriver
    extends RepartitionCorrectly.Tests
    with RepartitionCorrectly.AppTester
    with TopologyTestDriverKafkaTester {
  override def makeTestInputTopic(name: String) = MockInputTopic(name, 2)
  override def makeTestOutputTopic(name: String) = MockOutputTopic(name, 2)
}

class RepartitionCorrectlyAppTestsWithEmbeddedKafka
    extends RepartitionCorrectly.Tests
    with RepartitionCorrectly.AppTester
    with EmbeddedKafkaTester {
  override def makeTestInputTopic(name: String) = EmbeddedKafkaInputTopic(name, 2)
  override def makeTestOutputTopic(name: String) = EmbeddedKafkaOutputTopic(name, 2)
}
