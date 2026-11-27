package com.gestamp.hypatia.exceptions

class SparkconfigurationException(private val message: String = "",
                                  private val cause: Throwable = None.orNull)
  extends ExceptionService(message, cause)
