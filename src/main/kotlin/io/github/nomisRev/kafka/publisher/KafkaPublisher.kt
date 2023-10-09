package io.github.nomisRev.kafka.Flow

import io.github.nomisRev.kafka.publisher.DefaultKafkaPublisher
import io.github.nomisRev.kafka.publisher.PublisherRecord
import io.github.nomisRev.kafka.publisher.PublisherSettings
import io.github.nomisRev.kafka.publisher.RecordAck
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.internals.TransactionManager

/**
 * Reactive producer that sends outgoing records to topic partitions of a Kafka
 * cluster. The producer is thread-safe and can be used to publish records to
 * multiple partitions. It is recommended that a single [KafkaPublisher] is shared for each record
 * type in a client application.
 *
 * @param <K> outgoing record key type
 * @param <V> outgoing record value type
</V></K> */
interface KafkaPublisher<Key, Value> : AutoCloseable {
  /**
   * Sends a sequence of records to Kafka and returns a [Flow] of response record metadata including
   * partition and offset of each record. Responses are ordered for each partition in the absence of retries,
   * but responses from different partitions may be interleaved in a different order from the requests.
   * Additional correlation metadata may be passed through in the [SenderRecord] that is not sent
   * to Kafka, but is included in the response [Flow] to match responses to requests.
   *
   *
   * Results are published when the send is acknowledged based on the acknowledgement mode
   * configured using the option [ProducerConfig.ACKS_CONFIG]. If acks=0, records are acknowledged
   * after the requests are buffered without waiting for any server acknowledgements. In this case the
   * requests are not retried and the offset returned in [SenderResult] will be -1. For other ack
   * modes, requests are retried up to the configured [ProducerConfig.RETRIES_CONFIG] times. If
   * the request does not succeed after these attempts, the request fails and an exception indicating
   * the reason for failure is returned in [SenderResult.exception].
   * [SenderOptions.stopOnError] can be configured to stop the send sequence on first failure
   * or to attempt all sends even if one or more records could not be delivered.
   *
   *
   *
   * Example usage:
   * <pre>
   * `source = Flow.range(1, count)
   * .map(i -> SenderRecord.create(topic, partition, null, key(i), message(i), i));
   * sender.send(source, true)
   * .doOnNext(r -> System.out.println("Message #" + r.correlationMetadata() + " metadata=" + r.recordMetadata()));
  ` *
  </pre> *
   *
   * @param records Outbound records along with additional correlation metadata to be included in response
   * @return Flow of Kafka producer response record metadata along with the corresponding request correlation metadata.
   * For records that could not be sent, the response contains an exception that indicates reason for failure.
   */
  fun <T> send(records: Flow<PublisherRecord<Key, Value, T>>): Flow<RecordAck<T>>

  /**
   * Sends records from each inner Flow of `records` within a transaction.
   * Each transaction is committed if all the records are successfully delivered to Kafka
   * and aborted if any of the records in that batch could not be delivered.
   *
   *
   * Example usage:
   * <pre>
   * `sender.sendTransactionally(outboundRecords.window(10));
  ` *
  </pre> *
   *
   *
   * @param records Outbound producer records along with correlation metadata to match results returned.
   * Records from each inner Flow are sent within a new transaction.
   * @return Flow of Kafka producer response record metadata along with the corresponding request correlation metadata.
   * Each inner Flow contains results of records sent within a transaction.
   * @throws IllegalStateException if the sender was created without setting a non-empty
   * {@value org.apache.kafka.clients.producer.ProducerConfig#TRANSACTIONAL_ID_CONFIG} in [SenderOptions]
   */
  fun <T> sendTransitionally(records: Flow<Flow<PublisherRecord<Key, Value, T>>>): Flow<Flow<RecordAck<T>>>

  /**
   * Returns the [TransactionManager] instance associated with this sender,
   * which may be used for fine-grained control over transaction states. Sender
   * must have been created with a non-empty transactional id by setting
   * {@value org.apache.kafka.clients.producer.ProducerConfig#TRANSACTIONAL_ID_CONFIG} in [SenderOptions].
   *
   *
   *
   * **Threading model for transactional sender:**
   *
   * Sends may be scheduled from multiple threads with a transactional sender similar
   * to non-transactional senders. But transaction control operations and offset commits on
   * [TransactionManager] must be serialized and no sends may be performed
   * while one of the transaction control operations is in progress.
   *
   * @return [TransactionManager] associated with this sender
   * @throws IllegalStateException if the sender was created without setting a non-empty
   * {@value org.apache.kafka.clients.producer.ProducerConfig#TRANSACTIONAL_ID_CONFIG} in [SenderOptions]
   */
  fun transactionManager(): TransactionManager?

  //private fun <T> transaction(
//  transactionRecords: Publisher<out SenderRecord<K?, V?, T>?>,
//  transactionBoundary: Sinks.Many<Any>
//): Flux<SenderResult<T>> {
//  return transactionManager()
//    .begin()
//    .thenMany(send<Any>(transactionRecords))
//    .concatWith(transactionManager().commit())
//    .concatWith(Mono.fromRunnable { transactionBoundary.emitNext(this, this) })
//    .onErrorResume { e -> transactionManager().abort().then(Mono.error(e)) }
//    .publishOn(senderOptions.scheduler())
//}


//  /**
//   * Creates a reactive gateway for outgoing Kafka records. Outgoing sends can be chained
//   * using [KafkaOutbound.send] or [KafkaSender.sendTransactionally].
//   * Like [Flow] and [Mono], subscribing to the tail [KafkaOutbound] will
//   * schedule all parent sends in the declaration order.
//   *
//   *
//   *
//   * Example usage:
//   * <pre>
//   * `kafkaSender.createOutbound()
//   * .send(Flow1)
//   * .send(Flow2)
//   * .send(Flow3)
//   * .then()
//   * .subscribe();
//  ` *
//  </pre> *
//   *
//   * @return chainable reactive gateway for outgoing Kafka producer records
//   */
//  fun createOutbound(): KafkaOutbound<Key, Value>?

  /**
   * Invokes the specified function on the Kafka [Producer] associated with this [KafkaPublisher].
   *
   * Example usage:
   * <pre>
   * `sender.doOnProducer(producer -> producer.partitionsFor(topic))
   * .doOnSuccess(partitions -> System.out.println("Partitions " + partitions));
   *
   *
   * Functions that are directly supported on the reactive [KafkaPublisher] interface (eg. send)
   * should not be invoked from `function`. The methods supported by
   * `doOnProducer` are:
   *
   *  * [Producer.sendOffsetsToTransaction]
   *  * [Producer.partitionsFor]
   *  * [Producer.metrics]
   *  * [Producer.flush]
   *
   *
   * @param function A function that takes Kafka Producer as parameter
   * @return Mono that completes with the value returned by `function`
   */
  suspend fun <T> withProducer(function: (Producer<Key, Value>) -> T): T

  /**
   * Closes this sender and the underlying Kafka producer and releases all resources allocated to it.
   */
  override fun close()

  companion object {
    /**
     * Creates a Kafka sender that appends records to Kafka topic partitions.
     *
     * @param options Configuration options of this sender. Changes made to the options
     * after the sender is created will not be used by the sender.
     * @param createProducer Create a custom producer other than the default.
     * @return new instance of Kafka sender
     */
    fun <K, V> create(
      options: PublisherSettings<K, V>,
      createProducer: suspend () -> Producer<K, V> =
        { KafkaProducer(options.properties(), options.keySerializer, options.valueSerializer) }
    ): KafkaPublisher<K, V> =
      DefaultKafkaPublisher(createProducer, options)
  }
}
