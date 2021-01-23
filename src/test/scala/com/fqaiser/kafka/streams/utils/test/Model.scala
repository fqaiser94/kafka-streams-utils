package com.fqaiser.kafka.streams.utils.test

final case class InputKey(str: String = "Hello")
final case class InputValue(num: Int)
final case class OutputKey(str: String = "Hello")
final case class OutputValue(num: Int)
