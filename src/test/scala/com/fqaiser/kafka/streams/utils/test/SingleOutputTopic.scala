package com.fqaiser.kafka.streams.utils.test

import io.github.azhur.kafkaserdeavro4s.Avro4sBinarySupport
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KeyValue, Topology}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

object SingleOutputTopic {

  object App extends Avro4sBinarySupport {
    def genTopology(inputTopicName: String, outputTopicName: String): Topology = {
      val builder = new StreamsBuilder()

      builder
        .stream[InputKey, InputValue](inputTopicName)
        .map((k, v) => (OutputKey(), OutputValue(v.num + 1)))
        .to(outputTopicName)

      builder.build()
    }
  }

  trait Tests extends AnyFeatureSpec with Matchers {
    self: AppTester with KafkaTester =>

    Feature("Basic App") {
      Scenario("Adds 1 to both key and value") {
        runKafkaTest {
          case (inputTopic, outputTopic) =>
            inputTopic.pipeInput(InputKey(), InputValue(1))
            topicShouldContainTheSameElementsAs(outputTopic, Seq(new KeyValue(OutputKey(), OutputValue(2))))
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
    override type TestFunctionEnvironment = (TestInputTopic, TestOutputTopic)

    def makeTestInputTopic(name: String): TestInputTopic
    def makeTestOutputTopic(name: String): TestOutputTopic
    override def testFunctionParams = (makeTestInputTopic(inputTopicName), makeTestOutputTopic(outputTopicName))

    override lazy val topology: Topology = App.genTopology(inputTopicName, outputTopicName)
  }
}

class SingleOutputTopicAppTestsWithTopologyTestDriver
    extends SingleOutputTopic.Tests
    with SingleOutputTopic.AppTester
    with TopologyTestDriverKafkaTester {
  override def makeTestInputTopic(name: String) = MockInputTopic(name)
  override def makeTestOutputTopic(name: String) = MockOutputTopic(name)
}

class SingleOutputTopicAppTestsWithEmbeddedKafka
    extends SingleOutputTopic.Tests
    with SingleOutputTopic.AppTester
    with EmbeddedKafkaTester {
  override def makeTestInputTopic(name: String) = EmbeddedKafkaInputTopic(name)
  override def makeTestOutputTopic(name: String) = EmbeddedKafkaOutputTopic(name)
}
