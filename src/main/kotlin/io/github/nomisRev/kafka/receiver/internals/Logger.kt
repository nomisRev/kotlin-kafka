package io.github.nomisRev.kafka.receiver.internals

// TODO replace with proper Logger infrastructure
internal object Logger {
  internal fun debug(msg: String): Unit = Unit
    // println("${Thread.currentThread().name} => DEBUG: $msg")
  
  internal fun trace(msg: String): Unit = Unit
    // println("${Thread.currentThread().name} => TRACE: $msg")
  
  internal fun error(msg: String, throwable: Throwable? = null): Unit {
    // println("${Thread.currentThread().name} => ERROR: $msg")
    // throwable?.printStackTrace()
  }
  
  internal fun warn(msg: String, throwable: Throwable? = null): Unit {
    // println("${Thread.currentThread().name} => WARN: $msg")
    // throwable?.printStackTrace()
  }
  
  fun isTraceEnabled(): Boolean = true
  fun isDebugEnabled(): Boolean = true
}

internal val log: Logger = Logger
