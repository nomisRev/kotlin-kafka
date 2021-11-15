import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer

// TODO refactor this to well typed ProducerSettings
@FlowPreview
suspend fun <A, B> Flow<ProducerRecord<A, B>>.produce(
  props: Properties,
  keyDeserializer: Serializer<A>,
  valueDeserializer: Serializer<B>
): Flow<RecordMetadata> =
  kafkaProducer(props, keyDeserializer, valueDeserializer).flatMapConcat { producer ->
    this@produce.map { record ->
      producer.sendAwait(record)
    }
  }

/**
 * Sends a record to a Kafka topic in a suspending way.
 *
 * ```kotlin
 * KafkaProducer(Properties(), StringSerializer(), StringSerializer()).use { producer ->
 *   producer.sendAwait(ProducerRecord("topic-name", "message #1"))
 *   producer.sendAwait(ProducerRecord("topic-name", "message #2"))
 * }
 * ```
 * TODO properly support interruptible `send`, whilst still working in a suspendig way.
 *      See [kotlinx.coroutines.runInterruptible]
 */
suspend fun <A, B> KafkaProducer<A, B>.sendAwait(record: ProducerRecord<A, B>): RecordMetadata =
  suspendCoroutine { cont ->
    send(record) { a, e ->
      // null if an error occurred, see: org.apache.kafka.clients.producer.Callback
      if (a != null) cont.resume(a) else cont.resumeWithException(e)
    }
  }

/** // TODO Support Transactional behavior
 * Kafka producer as a [Flow]. Will automatically close, and flush when finished streaming.
 * The [Flow] will close when the [KafkaProducer] is consumed from the [Flow].
 *
 * This means that the [KafkaProducer] will not be closed for a synchronous running stream,
 * but when running the [Flow] is offloaded in a separate Coroutine it's prone to be collected, closed and flushed.
 * In the example below we construct a producer stream that produces 100 indexed messages.
 *
 * ```kotlin
 * fun <Key, Value> KafkaProducer<Key, Value>.produce(topicName: String, count: Int): Flow<Unit> =
 *   (0..count).asFlow().map { sendAwait(ProducerRecord(topicName, "message #it")) }
 *
 * val producerStream = kafkaProducer(Properties(), StringSerializer(), StringSerializer())
 *   .flatMapConcat { producer -> producer.produce("topic-name", 100) }
 * ```
 *
 * Here the `KafkaProducer` will only get collected (and closed/flushed) when all 100 messages were produced.
 *
 * **DO NOT**
 * If instead we'd do something like the following,
 * where we offload in a buffer then the `KafkaProducer` gets collected into the buffer and thus closed/flushed.
 *
 * ```kotlin
 * kafkaProducer(Properties(), StringSerializer(), StringSerializer()).buffer(10)
 * ```
 */
fun <Key, Value> kafkaProducer(
  props: Properties,
  keyDeserializer: Serializer<Key>,
  valueDeserializer: Serializer<Value>
): Flow<KafkaProducer<Key, Value>> =
  flow {
    val producer = KafkaProducer(props, keyDeserializer, valueDeserializer)
    try {
      producer.use { emit(it) }
    } finally {
      producer.flush()
    }
  }
