package io.github.nomisRev.kafka.receiver

import org.apache.kafka.clients.consumer.KafkaConsumer
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.buffer
import org.apache.kafka.common.TopicPartition

/**
 * When a [KafkaReceiver] does not process message fast enough,
 * it will start to buffer messages in a [channelFlow] using the default [Channel.BUFFERED].
 * This can be overwritten by using [buffer].
 *
 * When the [Channel] is full,
 * it will prevent the [KafkaReceiver] from calling [KafkaConsumer.poll] again.
 *
 * When the [KafkaConsumer] does not call [KafkaConsumer.poll] every `max.poll.interval.ms`,
 * then Kafka will consider our consumer dead and will rebalance the partitions.
 *
 * [RebalanceStrategy] specifies with which strategy the backpressure should be applied.
 */
public sealed interface RebalanceStrategy {
  /**
   * When we want to apply backpressure to the Kafka we need to _pause_ the partitions.
   * This will prevent Kafka from considering our consumer dead.
   *
   * Whenever we have space in the [Channel] we will _resume_ the partitions.
   *
   * NOTE: It can happen that with a slow processor you constantly pause and resume the partitions,
   * which can cause considerable overhead to Kafka.
   * Consider adding _logging_ or _alerting_ to your [RebalanceListener] to see how often this happens.
   */
  public object Backpressure : RebalanceStrategy
  
  /**
   * In some cases you **do not** want to apply automatic backpressure to Kafka.
   * For example when you need to guarantee _high throughput_,
   * and you **never** want a _pause_ or _resume_ causing a rebalance.
   *
   * In this scenario you can choose
   */
  public class FailFast(
    public val failFast: (Set<TopicPartition>) -> Nothing = { throw BackpressureException(it) }
  ) : RebalanceStrategy
}

public class BackpressureException(
  public val partitions: Set<TopicPartition>
) : Exception("KafkaConsumer is not processing messages fast enough for partitions: $partitions")
