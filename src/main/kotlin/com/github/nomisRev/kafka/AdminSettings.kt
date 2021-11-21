package com.github.nomisRev.kafka

import java.util.Properties
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsResult
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription

public data class AdminSettings(
  val bootStrapServer: String,
  private val props: Properties? = null
) {
  public fun properties(): Properties =
    Properties().apply {
      props?.let { putAll(it) }
      put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
    }
}

/** Construct an [Admin] with [AdminSettings] */
public fun Admin(settings: AdminSettings): Admin = Admin.create(settings.properties())

public suspend fun Admin.createTopic(
  topic: NewTopic,
  option: CreateTopicsOptions = CreateTopicsOptions()
): Unit {
  createTopics(listOf(topic), option).all().await()
}

public suspend fun Admin.deleteTopic(
  name: String,
  options: DeleteTopicsOptions = DeleteTopicsOptions()
): DeleteTopicsResult = deleteTopics(listOf(name), options)

public suspend fun Admin.describeTopic(
  name: String,
  options: DescribeTopicsOptions = DescribeTopicsOptions()
): TopicDescription? =
  describeTopics(listOf(name), options).values().getOrDefault(name, null)?.await()
