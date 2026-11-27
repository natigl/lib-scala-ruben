package com.gestamp.hypatia.utils

case class Action(action_name: String, parameters: Map[String, Any])
case class Table(table_name: String, actions: List[Action])
