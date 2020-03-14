package com.wkshi.lendingClub

import com.wkshi.lendingClub.aggregator.LoanInfoAggregator
import com.wkshi.lendingClub.io.{LoanAggregationWriter, LoanReader, RejectionReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object LoanAnalyze extends Logging with LoanReader with RejectionReader
  with LoanInfoAggregator with LoanAggregationWriter{

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logError("I am expecting 3 input")
    }

    val spark = SparkSession.builder().appName("Loan-analyze").getOrCreate()

    val loanInputPath = args(0)
    val rejectionPath = args(1)
    val outputPath = args(2)

    val loanDs = readLoanData(loanInputPath, spark)
    val rejectionDs = readRejectionData(rejectionPath, spark)
    val aggregateDf = loanInfoAggregator(rejectionDs, loanDs, spark)

    writeLoanAggregateData(aggregateDf, outputPath)
  }

}
