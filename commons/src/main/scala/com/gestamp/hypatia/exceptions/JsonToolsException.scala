package com.gestamp.hypatia.exceptions

class JsonToolsException(private val message: String = "",
                         private val cause: Throwable = None.orNull)
  extends ExceptionService(message, cause)
