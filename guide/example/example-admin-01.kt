package example.exampleAdmin01

import com.github.nomisRev.kafka.*
import java.util.Properties
import kotlinx.coroutines.runBlocking

import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG
import org.apache.kafka.clients.admin.NewTopic

fun main() = runBlocking {
  val settings = AdminSettings(
    Kafka.container.bootstrapServers,
    Properties().apply {
      put(CLIENT_DNS_LOOKUP_CONFIG, ClientDnsLookup.USE_ALL_DNS_IPS)
    }
  )
  Admin(settings).use { admin ->
    admin.createTopic(NewTopic("admin-settings-example", 1, 1))
    val topics = admin.listTopics().namesToListings().await()
    println(topics)
    admin.deleteTopic("admin-settings-example")
  }
}
