package spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util.Properties

/*
  1.spark版本变更为2.3.3，部署模式local即可。也可探索其他模式。
  2.由于远程调试出现的各种问题，且远程调试并非作业重点，这里重新建议使用spark-submit方式
  3.本代码及spark命令均为最简单配置。如运行出现资源问题，请根据你的机器情况调整conf的配置以及spark-submit的参数，具体指分配CPU核数和分配内存。

  调试：
    当前代码中集成了spark-sql，可在开发机如windows运行调试;
    需要在开发机本地下载hadoop，因为hadoop基于Linux编写，在开发机本地调试需要其中的一些文件，如模拟Linux目录系统的winutils.exe；
    请修改System.setProperty("hadoop.home.dir", "your hadoop path in windows like E:\\hadoop-x.x.x")

  部署：
    注释掉System.setProperty("hadoop.home.dir", "your hadoop path in windows like E:\\hadoop-x.x.x")；
    修改pom.xml中<scope.mode>compile</scope.mode>为<scope.mode>provided</scope.mode>
    打包 mvn clean package
    上传到你的Linux机器

    注意在~/base_profile文件中配置$SPARK_HOME,并source ~/base_profile,或在bin目录下启动spark-submit
    spark-submit Spark2DB-1.0.jar
 */


object Hive2 {
    // parameters
    LoggerUtil.setSparkLogLevels()
    val CH_Driver = "ru.yandex.clickhouse.ClickHouseDriver"
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
            .setAppName(this.getClass.getSimpleName)
            .setMaster("local[*]")

        val session = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        //executor内存尽可能大一些
        session.conf.set("spark.sql.debug.maxToStringFields", 1000)
        session.conf.set("spark.executor.memory", "4g")

        val reader = session.read.format("jdbc")
            .option("url", "jdbc:hive2://172.29.4.17:10000/default")
            .option("user", "student")
            .option("password", "nju2023")
            .option("driver", "org.apache.hive.jdbc.HiveDriver")

        val registerHiveDqlDialect = new RegisterHiveSqlDialect()
        registerHiveDqlDialect.register()

        val CHUrl = "jdbc:clickhouse://localhost:8123/dm"

        val CHProperty = new Properties()
        CHProperty.put("user", "default")
        CHProperty.put("password", "123456")
        CHProperty.put("driver", CH_Driver)

        val newTblStructure = new StructType()
          .add(StructField("uid", StringType, nullable = false))
          .add(StructField("contact_phone", StringType, nullable = true))
          .add(StructField("contact_address", StringType, nullable = true))

        val tblNameDsts = List("pri_cust_contact_info")


        for (tblNameDst <- tblNameDsts) {
            var df = reader.option("dbtable", tblNameDst).load()
            val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
            df = df.toDF(columnNames: _*)

            // code

            val invalidRow = List("", "-")
            val phoneType = List("TEL", "OTH", "MOB")

            val rdd: RDD[Row] = df.select("uid", "con_type", "contact").dropDuplicates().rdd
              .filter(row => (
                !invalidRow.contains(row.getString(row.fieldIndex("contact")))
                  && row.getString(row.fieldIndex("contact")).nonEmpty
                ))

            val reducePhone = rdd
              .filter(row => phoneType.contains(row.getString(row.fieldIndex("con_type"))))
              .map(row => (row.getString(row.fieldIndex("uid")), row.getString(row.fieldIndex("contact"))))
              .reduceByKey((x, y) => {
                  x + "," + y
              }).map(pair => (pair._1, (pair._2, "")))

            val reduceAddr = rdd
              .filter(row => !phoneType.contains(row.getString(row.fieldIndex("con_type"))))
              .map(row => (row.getString(row.fieldIndex("uid")), row.getString(row.fieldIndex("contact"))))
              .reduceByKey((x, y) => {
                  x + "," + y
              }).map(pair => (pair._1, ("", pair._2)))
            val newData = reducePhone.union(reduceAddr)
              .reduceByKey((row1, row2) =>
                  (row1._1 + row2._1, row1._2 + row2._2)
              )
              .map(row => Row(row._1, row._2._1, row._2._2))

            val tblData = session.createDataFrame(newData, newTblStructure)



            // 写入clickhouse
            // 写入参数
            val write_maps = Map[String, String](
                "batchsize" -> "2000",
                "isolationLevel" -> "NONE",
                "numPartitions" -> "1"
            )
            //      val pro = new Properties()
            //      pro.put("driver","ru.yandex.clickhouse.ClickHouseDriver")
            //      pro.put("driver","cc.blynk.clickhouse.ClickHouseDriver")
            // jdbc方式写入
            tblData.show(10)

            tblData
              .write
              .mode(SaveMode.Append)
              .options(write_maps)
              .jdbc("jdbc:clickhouse://localhost:8123/dm", "pri_cust_contact_info", CHProperty)

            //ETL end

            //2. 对其余9张表进行处理
            val otherTblNameDicts = List(
                "dm_v_as_djk_info",
                "dm_v_as_djkfq_info",
                "pri_cust_asset_acct_info",
                "pri_cust_asset_info",
                "pri_cust_base_info",
                "pri_cust_liab_acct_info",
                "pri_cust_liab_info",
                //"pri_cust_contact_info",
                "pri_credit_info",
                "pri_star_info")

            for (otherTblName <- otherTblNameDicts) {
                var df = reader.option("dbtable", otherTblName).load()
                // 去掉列名中的表名前缀，方便同学解析
                val columnNames = df.columns.toList.map(name => name.substring(otherTblName.length + 1)).toArray
                // 简单去重
                df = df.dropDuplicates()
                  .toDF(columnNames: _*)
                // 写入clickhouse
                // 写入参数
                val write_maps = Map[String, String](
                    "batchsize" -> "2000",
                    "isolationLevel" -> "NONE",
                    "numPartitions" -> "1"
                )

                // jdbc方式写入
                df.write.mode(SaveMode.Append)
                  .options(write_maps)
                  .jdbc(CHUrl, otherTblName, CHProperty)

            }
        }
        session.close()
    }

}
