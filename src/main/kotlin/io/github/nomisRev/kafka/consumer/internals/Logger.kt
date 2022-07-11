package io.github.nomisRev.kafka.consumer.internals

internal object Logger {
  internal fun debug(msg: String): Unit = println("DEBUG: $msg")
  internal fun trace(msg: String): Unit = println("TRACE: $msg")
  internal fun error(msg: String, throwable: Throwable? = null): Unit {
    println("ERROR: $msg")
    throwable?.printStackTrace()
  }
  internal fun warn(msg: String, throwable: Throwable? = null): Unit {
    println("WARN: $msg")
    throwable?.printStackTrace()
  }
  
  fun isTraceEnabled(): Boolean = true
  fun isDebugEnabled(): Boolean = true
}

internal val log: Logger = Logger
