package com.gestamp.hypatia.args

import com.gestamp.hypatia.postgresql.model.Usecase

/**
 * Case Class para mapear los parametros de entrada de la librería.
 *
 * @param idHypatiaManagerProcess

 */
case class JobParamsDeltaMonitoring(
                                     idHypatiaManagerProcess: String,
                                     usecase: Option[Usecase] = None
                                   )
