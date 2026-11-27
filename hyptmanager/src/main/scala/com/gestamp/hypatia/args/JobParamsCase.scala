package com.gestamp.hypatia.args

import com.gestamp.hypatia.postgresql.model.{Dataset, Distillation, Usecase}


/**
 * Case Class para mapear los parametros de entrada de la librería.
 *
 * @param idHypatiaManagerProcess
 * @param domain
 * @param subdomain
 * @param dataDate
 * @param datasets
 */
case class JobParamsCase(idHypatiaManagerProcess: String = "",
                         custom_params: Option[Map[String, Any]] = None,
                         use_case: Usecase = null,
                         spread: Boolean = false)