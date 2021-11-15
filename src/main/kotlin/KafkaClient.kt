import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import com.github.nomisRev.kafka.KafkaClientSettings
import com.github.nomisRev.kafka.asDeferred
import com.github.nomisRev.kafka.await
import java.util.Properties
import java.util.concurrent.ExecutionException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitAll
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsOptions
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.common.errors.TopicDeletionDisabledException
import org.apache.kafka.common.errors.TopicExistsException

/**
 * TODO REMOVE OR FINISH AND KEEP
 * Wrapping Kafka is probably not worth it, only providing utilities between KafkaFuture/Stream and KotlinX Coroutines
 */
interface KafkaClient {
  suspend fun Collection<NewTopic>.create(options: CreateTopicsOptions = CreateTopicsOptions()): Unit
  suspend fun deleteTopics(names: Collection<String>, options: DeleteTopicsOptions = DeleteTopicsOptions()): Unit
  suspend fun listTopics(options: ListTopicsOptions = ListTopicsOptions()): Map<String, TopicListing>
  fun describeTopicsDeferred(
    names: Collection<String>,
    options: DescribeTopicsOptions = DescribeTopicsOptions()
  ): Map<String, Deferred<TopicDescription>>

  suspend fun createTopics(topics: Collection<NewTopic>, options: CreateTopicsOptions = CreateTopicsOptions()): Unit =
    topics.create(options)

  suspend fun NewTopic.create(options: CreateTopicsOptions = CreateTopicsOptions()): Unit =
    listOf(this).create(options)

  suspend fun deleteTopic(name: String, options: DeleteTopicsOptions = DeleteTopicsOptions()) =
    deleteTopics(listOf(name), options)

  suspend fun describeTopic(name: String, options: DescribeTopicsOptions = DescribeTopicsOptions()): TopicDescription? =
    describeTopicsDeferred(listOf(name), options).getOrDefault(name, null)?.await()

  @OptIn(ExperimentalCoroutinesApi::class)
  suspend fun describeTopics(
    names: Collection<String>,
    options: DescribeTopicsOptions = DescribeTopicsOptions()
  ): Map<String, TopicDescription> {
    val res = describeTopicsDeferred(names)
    res.values.awaitAll()
    return res.mapValues { (_, deferred) -> deferred.getCompleted() }
  }
}

fun kafkaClient(settings: KafkaClientSettings): Resource<KafkaClient> =
  Resource.fromAutoCloseable { AdminClient.create(settings.props) }.map { client ->
    object : KafkaClient {
      override suspend fun Collection<NewTopic>.create(options: CreateTopicsOptions): Unit = try {
        client.createTopics(this, options).await()
        // Signal Topic Exists to user in a typed way. Other errors?? Not documented :(
      } catch (e: ExecutionException) {
        if (e.cause !is TopicExistsException) throw e
        else Unit
      }

      override suspend fun deleteTopics(names: Collection<String>, options: DeleteTopicsOptions): Unit {
        try {
          client.deleteTopics(names, options).all().await()
        } catch (e: ExecutionException) {
          // Horribly documented :'(
          if (e.cause !is TopicDeletionDisabledException) throw e
          else Unit
        }
      }

      override suspend fun listTopics(options: ListTopicsOptions): Map<String, TopicListing> =
        client.listTopics(options).namesToListings().await()

      override fun describeTopicsDeferred(
        names: Collection<String>,
        options: DescribeTopicsOptions
      ): Map<String, Deferred<TopicDescription>> =
        client.describeTopics(names, options).values().mapValues { (_, future) -> future.asDeferred() }
    }
  }