package io.github.nomisRev.kafka.internal

import java.util.Properties

/**
 * Reads a required, non-blank property.
 * Kafka allows list-typed properties such as `bootstrap.servers` to be configured as a [Collection],
 * so those are joined into a comma separated [String].
 *
 * @throws IllegalArgumentException when the property is missing, blank, or of an unsupported type.
 */
internal fun Properties.requireString(key: String): String {
  val value = when (val raw = get(key)) {
    null -> null
    is String -> raw
    is Collection<*> -> raw.joinToString(",")
    else -> throw IllegalArgumentException("Property '$key' must be a String or Collection but found ${raw::class.qualifiedName}")
  }
  require(!value.isNullOrBlank()) { "Missing required property '$key'" }
  return value
}

/**
 * Reads an optional property, and maps it to one of [values] by comparing against [value].
 *
 * @throws IllegalArgumentException when the property is present, but doesn't match any of [values].
 */
internal fun <A> Properties.optionalEnum(key: String, values: List<A>, value: (A) -> String): A? {
  val raw = get(key)?.toString()?.trim() ?: return null
  return requireNotNull(values.firstOrNull { value(it).equals(raw, ignoreCase = true) }) {
    "Invalid value '$raw' for property '$key', expected one of ${values.joinToString { value(it) }}"
  }
}
