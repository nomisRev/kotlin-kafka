package example.exampleAdmin01

import arrow.continuations.SuspendApp
import io.github.nomisRev.kafka.Admin
import io.github.nomisRev.kafka.AdminSettings
import io.github.nomisRev.kafka.await
import io.github.nomisRev.kafka.createTopic
import io.github.nomisRev.kafka.deleteTopic
import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG
import org.apache.kafka.clients.admin.NewTopic
import java.util.Properties

fun main() = SuspendApp {
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
