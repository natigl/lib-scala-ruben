package com.gestamp.hypatia.utils

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Utilidades de administración de Kafka.
 */
object KafkaHelper{

/**
 * Lista los topics existentes en el broker usando el AdminClient de Kafka.
 *
 * @param brokers bootstrap servers del broker
 * @return los nombres de los topics existentes
 */
def listKafkaTopics(brokers: String): Set[String] = {
  val props = new Properties()
  props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

  val adminClient = AdminClient.create(props)
  try {
    adminClient.listTopics().names().get().asScala.toSet
  } finally {
    adminClient.close()
  }
}
}