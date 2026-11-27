package com.gestamp.hypatia.exceptions

class DatabricksManagerException(private val message: String = "",
                                 private val cause: Throwable = None.orNull)
  extends ExceptionService(message, cause)
