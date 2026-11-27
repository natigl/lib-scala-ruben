package com.gestamp.hypatia.example

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaMaestrosConstants {

  val schema_age_range = new StructType()
    .add(StructField("ageRange_code", StringType, true))
    .add(StructField("itemVersion_code", StringType, true))
    .add(StructField("changed_code", StringType, true))
    .add(StructField("order", StringType, true))
    .add(StructField("ageRange_Desc", StringType, true))

  val schema_employee_group = new StructType()
    .add(StructField("employeeGroup", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("employeeGroup_Desc", StringType, true))

  val schema_gender = new StructType()
    .add(StructField("gender_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("gender_Desc", StringType, true))

  val schema_job = new StructType()
    .add(StructField("job_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("dateto", StringType, true))
    .add(StructField("dateFrom", StringType, true))
    .add(StructField("job_Descsh", StringType, true))
    .add(StructField("Job_Descmd", StringType, true))
    .add(StructField("compensationJobGroup", StringType, true))
    .add(StructField("subFamily_code", StringType, true))

  val schema_job_family = new StructType()
    .add(StructField("subfamily_code", StringType, true))
    .add(StructField("languageSubFamily_code", StringType, true))
    .add(StructField("datetoSubFamily", StringType, true))
    .add(StructField("dateFromSubFamily", StringType, true))
    .add(StructField("subFamily_descsh", StringType, true))
    .add(StructField("subFamily_Descmd", StringType, true))

  val schema_type_of_labour = new StructType()
    .add(StructField("typeOfLabour_code", StringType, true))
    .add(StructField("typeOfLabour_Descsh", StringType, true))
    .add(StructField("typeOfLabour_Descmd", StringType, true))

  val schema_seniority_segmentation = new StructType()
    .add(StructField("senioritySegmentation_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("senioritySegmentation_desc", StringType, true))
    .add(StructField("itemVersion_code", StringType, true))
    .add(StructField("changed_code", StringType, true))

  val schema_type_of_employment = new StructType()
    .add(StructField("typeOfEmployment_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("typeofEmployment_Desc", StringType, true))
    .add(StructField("itemVersion_code", StringType, true))
    .add(StructField("changed_code", StringType, true))
    .add(StructField("typeOfEmploymentGroup_code", StringType, true))

  val schema_action_type = new StructType()
    .add(StructField("actionType_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("actionType_Desc", StringType, true))

  val schema_action_reason = new StructType()
    .add(StructField("actionType_code", StringType, true))
    .add(StructField("actionReason_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("actionreason_desc", StringType, true))

  val schema_leaving_reason = new StructType()
    .add(StructField("actionType_code", StringType, true))
    .add(StructField("leavingReason_code", StringType, true))
    .add(StructField("itemVersion_code", StringType, true))
    .add(StructField("changed_code", StringType, true))
    .add(StructField("typeOfLeaving_code", StringType, true))
    .add(StructField("leavingReason_desc", StringType, true))
    .add(StructField("language_code", StringType, true))

  val schema_leaving_type = new StructType()
    .add(StructField("typeOfLeaving_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("typeOfLeaving_Desc", StringType, true))

  val schema_time_type = new StructType()
    .add(StructField("timeType_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("timeType_Desc", StringType, true))

  val schema_subtype_time = new StructType()
    .add(StructField("subtype_code", StringType, true))
    .add(StructField("language_code", StringType, true))
    .add(StructField("subtype_Descsh", StringType, true))
    .add(StructField("subtype_Desclg", StringType, true))

  val schema_hr_parameters = new StructType()
    .add(StructField("hrparameter_code", StringType, true))
    .add(StructField("hrparameter_value", StringType, true))


  val map_schemas = Map(
    "age_range" -> schema_age_range, "employee_group" -> schema_employee_group, "gender" -> schema_gender, "job" -> schema_job,
      "job_family" -> schema_job_family, "type_of_labour" -> schema_type_of_labour, "seniority_segmentation" -> schema_seniority_segmentation,
      "type_of_employment" -> schema_type_of_employment, "action_type" -> schema_action_type, "action_reason" -> schema_action_reason,
      "leaving_reason" -> schema_leaving_reason, "leaving_type" -> schema_leaving_type, "time_type" -> schema_time_type,
      "subtype_time" -> schema_subtype_time, "hr_parameters" -> schema_hr_parameters)

}
