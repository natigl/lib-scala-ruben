package com.gestamp.hypatia.exceptions

class ADLS2ManagerException(private val message: String = "",
                            private val cause: Throwable = None.orNull)
  extends ExceptionService(message, cause)
