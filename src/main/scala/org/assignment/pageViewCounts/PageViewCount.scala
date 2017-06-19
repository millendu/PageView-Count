package org.assignment.pageViewCounts

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ListBuffer

object PageViewCount {
  def main(args: Array[String]) = {
    //Start the Spark context
    try {
      val conf = new SparkConf()
        .setAppName("PageViewCounts")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val sqlcontext = new SQLContext(sc)
      val eventsFile = sc.textFile("ad-events_2014-01-20_00_domU-12-31-39-01-A1-34", 1)
      val assestsFile = sc.textFile("assets_2014-01-20_00_domU-12-31-39-01-A1-34", 1)
      val events = sqlcontext.read.json(eventsFile.map { x =>
        x.substring(StringUtils.ordinalIndexOf(x, "{", 2), StringUtils.ordinalIndexOf(x, "}", 2) + 1)
      }).registerTempTable("events")
      val assets = sqlcontext.read.json(assestsFile.map { x =>
        x.substring(StringUtils.ordinalIndexOf(x, "{", 2))
      }).registerTempTable("assets")

      val eventsDF = sqlcontext.sql("SELECT pv,e FROM events")
      val assetsDF = sqlcontext.sql("SELECT pv,'default' as e FROM assets")

      val eventOnlyDF = eventsDF.registerTempTable("events")
      val assetOnlyDF = assetsDF.registerTempTable("assets")
      
      val onlyViewCounts = sqlcontext.sql("SELECT pv as view_pv,count(*) as view_count FROM events where e='view' group by pv")
      val onlyClickCounts = sqlcontext.sql("SELECT pv as click_pv,count(*) as click_count FROM events where e='click' group by pv")
      val assetCounts = sqlcontext.sql("SELECT pv as pv,count(*)  as count FROM assets group by pv")
      
      assetCounts.join(onlyViewCounts, assetCounts("pv") === onlyViewCounts("view_pv"), "left_outer")
        .drop("view_pv")
        .join(onlyClickCounts, assetCounts("pv") === onlyClickCounts("click_pv"), "left_outer")
        .drop("click_pv").na.fill(0, Seq("view_count", "click_count")).rdd.coalesce(1, true).saveAsTextFile("output")

    } catch {
      case e: Exception =>
        throw e
    }

  }
}