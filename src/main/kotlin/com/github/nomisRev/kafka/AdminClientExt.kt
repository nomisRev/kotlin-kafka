package com.github.nomisRev.kafka

import java.util.Properties
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsResult
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription

// TODO write nice Kotlin DSL for writing Properties/or more domain specific Kafka utils?
data class KafkaClientSettings(
  val bootStrapServer: String,
  val props: Properties = Properties()
) {
  init {
    props.putIfAbsent(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
  }
}

suspend fun AdminClient.createTopic(topic: NewTopic, option: CreateTopicsOptions = CreateTopicsOptions()): Unit {
  createTopics(listOf(topic), option).all().await()
}

suspend fun AdminClient.deleteTopic(name: String, options: DeleteTopicsOptions = DeleteTopicsOptions()): DeleteTopicsResult =
  deleteTopics(listOf(name), options)

suspend fun AdminClient.describeTopic(name: String, options: DescribeTopicsOptions = DescribeTopicsOptions()): TopicDescription? =
  describeTopics(listOf(name), options).values().getOrDefault(name, null)?.await()