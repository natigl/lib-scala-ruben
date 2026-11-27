package com.gestamp.hypatia.integration.exception

class IntegrationParserArgsException(private val message: String = "",
                                     private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
