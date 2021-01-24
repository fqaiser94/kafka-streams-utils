package com.fqaiser.kafka.streams.utils.test

import io.github.azhur.kafkaserdeavro4s.Avro4sBinarySupport
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

object MultipleOutputTopics {

  object App extends Avro4sBinarySupport {
    def genTopology(
        inputTopicName: String,
        oddNumbersOutputTopicName: String,
        evenNumbersOutputTopicName: String
    ): Topology = {
      val builder = new StreamsBuilder()

      val numbers = builder.stream[InputKey, InputValue](inputTopicName)
      numbers.filter((k, v) => v.num % 2 == 0).to(evenNumbersOutputTopicName)
      numbers.filter((k, v) => v.num % 2 != 0).to(oddNumbersOutputTopicName)

      builder.build()
    }
  }

  trait Tests extends AnyFeatureSpec with Matchers {
    self: AppTester with KafkaTester =>

    Feature("Basic App") {
      Scenario("Adds 1 to both key and value") {
        runKafkaTest {
          case InputOutputTopics(
                Seq(inputTopic: TestInputTopic),
                Seq(
                  oddNumbersOutputTopic: TestOddNumbersOutputTopic,
                  evenNumbersOutputTopic: TestEvenNumbersOutputTopic
                )
              ) =>
            inputTopic.pipeInput(InputKey(), InputValue(1))
            inputTopic.pipeInput(InputKey(), InputValue(2))
            topicShouldContainTheSameElementsInOrderAs(
              oddNumbersOutputTopic,
              Seq(Record(OutputKey(), OutputValue(1), 0))
            )
            topicShouldContainTheSameElementsInOrderAs(
              evenNumbersOutputTopic,
              Seq(Record(OutputKey(), OutputValue(2), 0))
            )
        }
      }
    }
  }

  trait AppTester extends Avro4sBinarySupport {
    self: KafkaTester =>

    val inputTopicName = "InputTopic"
    val oddNumbersOutputTopicName = "OddNumbersOutputTopic"
    val evenNumbersOutputTopicName = "EvenNumbersOutputTopic"

    type TestInputTopic = InputTopic[InputKey, InputValue]
    type TestOddNumbersOutputTopic = OutputTopic[OutputKey, OutputValue]
    type TestEvenNumbersOutputTopic = OutputTopic[OutputKey, OutputValue]

    def makeInputTopic(name: String): TestInputTopic
    def makeOddNumbersOutputTopic(name: String): TestOddNumbersOutputTopic
    def makeEvenNumbersOutputTopic(name: String): TestEvenNumbersOutputTopic
    override def inputOutputTopics() =
      InputOutputTopics(
        makeInputTopic(inputTopicName) :: Nil,
        makeOddNumbersOutputTopic(oddNumbersOutputTopicName) :: makeEvenNumbersOutputTopic(
          evenNumbersOutputTopicName
        ) :: Nil
      )

    override lazy val topology: Topology =
      App.genTopology(inputTopicName, oddNumbersOutputTopicName, evenNumbersOutputTopicName)
  }
}

class MultipleOutputTopicsAppTestsWithTopologyTestDriver
    extends MultipleOutputTopics.Tests
    with MultipleOutputTopics.AppTester
    with TopologyTestDriverKafkaTester {
  override def makeInputTopic(name: String) = MockInputTopic(name)
  override def makeOddNumbersOutputTopic(name: String) = MockOutputTopic(name)
  override def makeEvenNumbersOutputTopic(name: String) = MockOutputTopic(name)
}

class MultipleOutputTopicsAppTestsWithEmbeddedKafka
    extends MultipleOutputTopics.Tests
    with MultipleOutputTopics.AppTester
    with EmbeddedKafkaTester {
  override def makeInputTopic(name: String) = EmbeddedKafkaInputTopic(name)
  override def makeOddNumbersOutputTopic(name: String) = EmbeddedKafkaOutputTopic(name)
  override def makeEvenNumbersOutputTopic(name: String) = EmbeddedKafkaOutputTopic(name)
}
