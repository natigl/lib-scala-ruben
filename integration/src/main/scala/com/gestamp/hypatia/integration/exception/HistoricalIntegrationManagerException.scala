package com.gestamp.hypatia.integration.exception

class HistoricalIntegrationManagerException(private val message: String = "",
                                            private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
