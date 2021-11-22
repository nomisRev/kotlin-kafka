@file:JvmName("KafkaFutureExt")
package com.github.nomisRev.kafka

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.DeleteTopicsResult
import org.apache.kafka.common.KafkaFuture

/** Await all [DeleteTopicsResult] in a suspending way. */
public suspend fun DeleteTopicsResult.await() {
  all().await()
}

/** Await all [CreateTopicsResult] in a suspending way. */
public suspend fun CreateTopicsResult.await() {
  all().await()
}

/**
 * Await a [KafkaFuture] in a suspending way. Code inspired by
 * [KotlinX Coroutines JDK8](https://github.com/Kotlin/kotlinx.coroutines/tree/master/integration/kotlinx-coroutines-jdk8)
 */
public suspend fun <T> KafkaFuture<T>.await(): T =
  toCompletionStage().await()

/**
 * Converts this [KafkaFuture] to an instance of [Deferred].
 *
 * The [KafkaFuture] is cancelled when the resulting deferred is cancelled.
 */
@OptIn(InternalCoroutinesApi::class)
@Suppress("DeferredIsResult")
public fun <T> KafkaFuture<T>.asDeferred(): Deferred<T> =
  toCompletionStage().asDeferred()
