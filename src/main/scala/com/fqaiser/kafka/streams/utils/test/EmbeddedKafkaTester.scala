package com.fqaiser.kafka.streams.utils.test

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}
import org.scalactic.source
import org.scalatest.compatible.Assertion
import org.scalatest.concurrent.Eventually.{PatienceConfig, eventually}
import org.scalatest.enablers.Retrying
import org.scalatest.time.{Seconds, Span}

import java.time.Duration
import java.util.{Collections, Properties}
import _root_.scala.collection.mutable.ArrayBuffer

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

  final case class EmbeddedKafkaInputTopic[K, V](name: String)(implicit keySer: Serializer[K], valSer: Serializer[V])
      extends InputTopic[K, V] {

    private val producerProps = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props
    }
    private val producer = new KafkaProducer(producerProps, keySer, valSer)

    override def pipeInput(key: K, value: V): Unit = producer.send(new ProducerRecord(name, key, value)).get
  }

  final case class EmbeddedKafkaOutputTopic[K, V](name: String)(implicit
      keyDes: Deserializer[K],
      valDes: Deserializer[V]
  ) extends OutputTopic[K, V] {

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

    override def readKeyValuesToList(): List[KeyValue[K, V]] = {
      val consumer = newConsumer()
      consumer.subscribe(Collections.singletonList(name))
      val records = consumer.poll(Duration.ofSeconds(10))
      val res = ArrayBuffer[KeyValue[K, V]]()
      records.forEach(cr => res.append(new KeyValue(cr.key(), cr.value())))
      res.toList
    }
  }

  override final def topicShouldContainTheSameElementsAs[K, V](
      outputTopic: OutputTopic[K, V],
      expected: Seq[KeyValue[K, V]]
  ): Assertion = {
    eventually {
      super.topicShouldContainTheSameElementsAs(outputTopic, expected)
    }(patienceConfig, implicitly[Retrying[Assertion]], implicitly[source.Position])
  }

}
