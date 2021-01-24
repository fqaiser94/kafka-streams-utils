package com.fqaiser.kafka.streams.utils.test

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.scalactic.source
import org.scalatest.compatible.Assertion
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually}
import org.scalatest.enablers.Retrying
import org.scalatest.time.{Seconds, Span}

import java.time.Duration
import java.util.{Collections, Properties}
import _root_.scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scala.util.{Failure, Success}

trait EmbeddedKafkaTester extends KafkaTester with EmbeddedKafka {
  val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span.apply(20, Seconds), // total/max amount of time to wait for something to succeed eventually
    interval = Span.apply(5, Seconds) // amount of time to wait between unsuccessful attempts
  )

  val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
  private val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"

  val applicationId = "my-app"

  override final def buildStream(topology: Topology): Stream = {
    val kstream = {
      val streamsConfig = {
        val props = new Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
        // do not remove; this forces KafkaStreams apps to process one record at a time basically
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        props
      }
      new KafkaStreams(topology, streamsConfig)
    }

    new Stream {
      override def start(): Unit = {
        kstream.cleanUp()
        kstream.start()
      }

      override def close(): Unit = kstream.close()
    }
  }

  override final def runKafkaTest(testFunction: TestFunction): Unit = {
    withRunningKafka {
      super.runKafkaTest(testFunction)
    }(embeddedKafkaConfig)
  }

  final case class EmbeddedKafkaInputTopic[K, V](name: String, numPartitions: Int = 1)(implicit
      keySer: Serializer[K],
      valSer: Serializer[V]
  ) extends InputTopic[K, V] {

    import org.apache.kafka.common.errors.TopicExistsException
    import java.util.concurrent.ExecutionException

    // TODO: this is a bit flaky so currently we retry until success
    //  Ideally, just make this not flaky
    private def createTopic() =
      withAdminClient(
        _.createTopics(Collections.singleton(new NewTopic(name, numPartitions, 1.toShort))).all().get()
      )(embeddedKafkaConfig)

    @tailrec
    private def retryCreateTopicUntilSuccess(attemptsLeft: Int = 10): Unit = {
      println(s"attemptsLeft: $attemptsLeft")
      val res = createTopic()
      println(s"res: $res")
      res match {
        case Success(_)                                                                      => Unit
        case Failure(e: ExecutionException) if e.getCause.isInstanceOf[TopicExistsException] => Unit
        case Failure(e) if attemptsLeft > 0                                                  => retryCreateTopicUntilSuccess(attemptsLeft - 1)
        case Failure(exception)                                                              => throw exception
      }
    }

    override def create(): Unit = retryCreateTopicUntilSuccess()

    // TODO: getting this error occassionally with RepartitionCorrectlyAppTestsWithEmbeddedKafka
    //  [info]   java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Topic InputTopic not present in metadata after 60000 ms.
    override def pipeInput(key: K, value: V, partition: Integer = null): Unit =
      producer.send(new ProducerRecord(name, partition, key, value)).get

    private val producerProps = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(ProducerConfig.ACKS_CONFIG, "all")
      props
    }

    private val producer = new KafkaProducer(producerProps, keySer, valSer)
  }

  final case class EmbeddedKafkaOutputTopic[K, V](name: String, numPartitions: Int = 1)(implicit
      keyDes: Deserializer[K],
      valDes: Deserializer[V]
  ) extends OutputTopic[K, V] {

    // TODO: can be reused across InputTopic and OutputTopic
    override def create(): Unit =
      withAdminClient(
        _.createTopics(Collections.singletonList(new NewTopic(name, numPartitions, 1.toShort))).all().get()
      )

    override def readKeyValuesToList(): List[Record[K, V]] = {
      val consumer = newConsumer()
      consumer.subscribe(Collections.singletonList(name))
      val records = consumer.poll(Duration.ofSeconds(10))
      val res = ArrayBuffer[Record[K, V]]()
      records.forEach(cr => res.append(Record(cr.key(), cr.value(), cr.partition())))
      res.toList
    }

    private def newConsumer() = {
      val consumerProps = {
        val props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, java.util.UUID.randomUUID.toString)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props
      }
      new KafkaConsumer(consumerProps, keyDes, valDes)
    }
  }

  override final def topicShouldContainTheSameElementsInOrderAs[K, V](
      outputTopic: OutputTopic[K, V],
      expected: Seq[Record[K, V]]
  ): Assertion = {
    eventually {
      super.topicShouldContainTheSameElementsInOrderAs(outputTopic, expected)
    }(patienceConfig, implicitly[Retrying[Assertion]], implicitly[source.Position])
  }

}
