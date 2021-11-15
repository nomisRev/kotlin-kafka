import java.time.Duration
import java.util.Properties
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer

/**
 *
 */
fun <K, V> kafkaConsumer(
  props: Properties,
  keyDeserilizer: Deserializer<K>,
  valueDeserilizer: Deserializer<V>
): Flow<KafkaConsumer<K, V>> = flow {
  KafkaConsumer(props, keyDeserilizer, valueDeserilizer).use { emit(it) }
}

@OptIn(FlowPreview::class)
fun <K, V> Flow<KafkaConsumer<K, V>>.subscribeTo(
  name: String,
  timeout: Duration = Duration.ofMillis(500)
): Flow<ConsumerRecord<K, V>> = flatMapConcat { consumer ->
  flow {
    consumer.subscribe(listOf(name))
    while (true) {
      consumer.poll(timeout).forEach { record -> emit(record) }
    }
  }
}
