package io.github.nomisrev.kafka

expect interface Header {
  val key: String?
  val value: ByteArray?
}

typealias Headers = MutableList<Header>
