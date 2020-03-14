package com.wkshi.lendingClub.io

import com.wkshi.lendingClub.types.LoanType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

trait RejectionReader extends Logging{

  def readRejectionData(inputPath: String, spark: SparkSession): Dataset[LoanType] = {
    import spark.implicits._

    var rawData = spark.read.option("header", "true").csv(inputPath)

    logInfo("reading data from %s".format(inputPath))

    rawData.select("Amount Requested", "Loan Title", "Debt-To-Income Ratio", "State", "Employment Length")
      .withColumnRenamed("Amount Requested", "loan_amnt")
      .withColumnRenamed("Loan Title", "title")
      .withColumnRenamed("Debt-To-Income Ratio", "DTI")
      .withColumnRenamed("State", "addr_state")
      .withColumnRenamed("Employment Length", "emp_length")
      .withColumn("term", lit(null: StringType))
      .withColumn("int_rate", lit(null: StringType))
      .withColumn("installment", lit(null: StringType))
      .withColumn("home_ownership", lit(null: StringType))
      .withColumn("annual_inc", lit(null: StringType))
      .withColumn("has_collection", lit(0))
      .as[LoanType]
  }
}
