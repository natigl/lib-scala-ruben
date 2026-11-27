package com.gestamp.hypatia.exceptions

class HypatiaManagerParserException(private val message: String = "",
                                    private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
