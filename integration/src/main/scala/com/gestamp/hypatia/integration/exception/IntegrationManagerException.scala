package com.gestamp.hypatia.integration.exception

class IntegrationManagerException(private val message: String = "",
                                  private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
