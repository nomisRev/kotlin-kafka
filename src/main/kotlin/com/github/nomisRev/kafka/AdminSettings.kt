package com.github.nomisRev.kafka

import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import java.util.Properties
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsResult
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription

data class AdminSettings(val bootStrapServer: String, private val props: Properties? = null) {
  fun properties(): Properties =
    Properties().apply {
      props?.let { putAll(it) }
      put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
    }
}

/** Construct a AdminClient as a Resource */
fun adminClient(settings: AdminSettings): Resource<AdminClient> =
  Resource.fromAutoCloseable { AdminClient.create(settings.properties()) }

suspend fun AdminClient.createTopic(
  topic: NewTopic,
  option: CreateTopicsOptions = CreateTopicsOptions()
): Unit {
  createTopics(listOf(topic), option).all().await()
}

suspend fun AdminClient.deleteTopic(
  name: String,
  options: DeleteTopicsOptions = DeleteTopicsOptions()
): DeleteTopicsResult = deleteTopics(listOf(name), options)

suspend fun AdminClient.describeTopic(
  name: String,
  options: DescribeTopicsOptions = DescribeTopicsOptions()
): TopicDescription? =
  describeTopics(listOf(name), options).values().getOrDefault(name, null)?.await()
