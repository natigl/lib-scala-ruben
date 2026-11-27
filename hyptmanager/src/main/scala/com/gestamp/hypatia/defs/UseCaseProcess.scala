package com.gestamp.hypatia.defs

/**
 * Represents a Hypatia Process.
 *
 * @param entities The list of tables of the process.
 * @param parallelProcessing If entities must be processed in parallel.
 */
case class UseCaseProcess(entities: List[Entity], parallelProcessing : Boolean = false )
