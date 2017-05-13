package lib

import java.io.File
import javax.ws.rs.QueryParam

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.redislabs.provider.redis._
import org.apache.spark.sql.types._

import scala.collection.mutable.HashMap


class query_engine_v2( sc: SparkContext,  sqlContext: SQLContext) extends query_engine(sc,sqlContext)
{

   //star_name  9_136 ab_ref_1_172030
  def readFromRedis_basic_time_spatial_index(star_name:String): Unit ={


          val star_type=star_name.split("_")(0)
          val ccd_num=star_name.split("_")(1)

          if (star_type=="ab")
          {
            //-----abnomal star-----//

            val zsetRDD = sc.fromRedisZRange(star_name, 0, -1) //.collect()

            def myfuncPerPartition(iter: Iterator[String]): Iterator[Row] = {
              println("run in partition")

              //            var res = List[String]()
              //            while (iter.hasNext)
              //            {
              //              val cur = iter.next;
              //
              //              res
              //            }
              //            res.iterator

              var  res = for (line <- iter ) yield {
                val p=line.split(" ")

                Row(
                  if (p(0) == null) null else p(0),
                  if (p(1) == null) null else p(1).toInt,
                  if (p(2) == null) null else p(2).toInt,
                  if (p(3) == null) null else p(3).toInt,
                  if (p(4) == null) null else p(4).toDouble,
                  if (p(5) == null) null else p(5).toDouble,
                  if (p(6) == null) null else p(6).toDouble,
                  if (p(7) == null) null else p(7).toDouble,
                  if (p(8) == null) null else p(8).toDouble,
                  if (p(9) == null) null else p(9).toDouble,
                  if (p(10) == null) null else p(10).toDouble,
                  if (p(11) == null) null else p(11).toDouble,
                  if (p(12) == null) null else p(12).toDouble,
                  if (p(13) == null) null else p(13).toDouble,
                  if (p(14) == null) null else p(14).toDouble,
                  if (p(15) == null) null else p(15).toDouble,
                  if (p(16) == null) null else p(16).toDouble,
                  if (p(17) == null) null else p(17).toDouble,
                  if (p(18) == null) null else p(18).toDouble,
                  if (p(19) == null) null else p(19).toDouble,
                  if (p(20) == null) null else p(20).toDouble,
                  if (p(21) == null) null else p(21).toDouble,
                  if (p(22) == null) null else p(22).toDouble,
                  if (p(23) == null) null else p(23).toInt,
                  if (p(24) == null) null else p(24).toInt)
              }
              res
            }

            val RDD_Row = zsetRDD.map( line => line.split(" ")).map { p => //p is one line
              Row(
                if (p(0) == null) null else p(0),
                if (p(1) == null) null else p(1).toInt,
                if (p(2) == null) null else p(2).toInt,
                if (p(3) == null) null else p(3).toInt,
                if (p(4) == null) null else p(4).toDouble,
                if (p(5) == null) null else p(5).toDouble,
                if (p(6) == null) null else p(6).toDouble,
                if (p(7) == null) null else p(7).toDouble,
                if (p(8) == null) null else p(8).toDouble,
                if (p(9) == null) null else p(9).toDouble,
                if (p(10) == null) null else p(10).toDouble,
                if (p(11) == null) null else p(11).toDouble,
                if (p(12) == null) null else p(12).toDouble,
                if (p(13) == null) null else p(13).toDouble,
                if (p(14) == null) null else p(14).toDouble,
                if (p(15) == null) null else p(15).toDouble,
                if (p(16) == null) null else p(16).toDouble,
                if (p(17) == null) null else p(17).toDouble,
                if (p(18) == null) null else p(18).toDouble,
                if (p(19) == null) null else p(19).toDouble,
                if (p(20) == null) null else p(20).toDouble,
                if (p(21) == null) null else p(21).toDouble,
                if (p(22) == null) null else p(22).toDouble,
                if (p(23) == null) null else p(23).toInt,
                if (p(24) == null) null else p(24).toInt)
            }


            //          val RDD_Row_2 = zsetRDD.mapPartitions(myfuncPerPartition)

            //          RDD_Row_2.collect().foreach(println)

            val RDD_Row_1 = zsetRDD.map { line =>
              val p = line.split(" ") //p is one line
              Row(
                if (p(0) == null) null else p(0),
                if (p(1) == null) null else p(1).toInt,
                if (p(2) == null) null else p(2).toInt,
                if (p(3) == null) null else p(3).toInt,
                if (p(4) == null) null else p(4).toDouble,
                if (p(5) == null) null else p(5).toDouble,
                if (p(6) == null) null else p(6).toDouble,
                if (p(7) == null) null else p(7).toDouble,
                if (p(8) == null) null else p(8).toDouble,
                if (p(9) == null) null else p(9).toDouble,
                if (p(10) == null) null else p(10).toDouble,
                if (p(11) == null) null else p(11).toDouble,
                if (p(12) == null) null else p(12).toDouble,
                if (p(13) == null) null else p(13).toDouble,
                if (p(14) == null) null else p(14).toDouble,
                if (p(15) == null) null else p(15).toDouble,
                if (p(16) == null) null else p(16).toDouble,
                if (p(17) == null) null else p(17).toDouble,
                if (p(18) == null) null else p(18).toDouble,
                if (p(19) == null) null else p(19).toDouble,
                if (p(20) == null) null else p(20).toDouble,
                if (p(21) == null) null else p(21).toDouble,
                if (p(22) == null) null else p(22).toDouble,
                if (p(23) == null) null else p(23).toInt,
                if (p(24) == null) null else p(24).toInt)

            }



            val tableStruct =
              StructType(Array(
                StructField("star_id", StringType, true),
                StructField("ccd_num", IntegerType, true),
                StructField("imageid", IntegerType, true),
                StructField("zone", IntegerType, true),
                StructField("ra", DoubleType, true),
                StructField("dec", DoubleType, true),
                StructField("mag", DoubleType, true),
                StructField("x_pix", DoubleType, true),
                StructField("y_pix", DoubleType, true),
                StructField("ra_err", DoubleType, true),
                StructField("dec_err", DoubleType, true),
                StructField("x", DoubleType, true),
                StructField("y", DoubleType, true),
                StructField("z", DoubleType, true),
                StructField("flux", DoubleType, true),
                StructField("flux_err", DoubleType, true),
                StructField("normmag", DoubleType, true),
                StructField("flag", DoubleType, true),
                StructField("background", DoubleType, true),
                StructField("threshold", DoubleType, true),
                StructField("mag_err", DoubleType, true),
                StructField("ellipticity", DoubleType, true),
                StructField("class_star", DoubleType, true),
                StructField("orig_catid", IntegerType, true),
                StructField("timestamp", IntegerType, true)
              ))
            sqlContext.createDataFrame(RDD_Row_1, tableStruct).registerTempTable(star_name)
            sqlContext.sql("SELECT * FROM "+star_name).show()


          }

          else if (star_type=="block")
          {
            val block_name=star_name
            //-----nomal star------//

            val zsetRDD = sc.fromRedisZRange(block_name, 0, 1) //.collect()

            val list_blocks = zsetRDD.map { one_15s_block =>

              val lines = one_15s_block.split("\n")

              val Array_Row = lines.map(line => line.split(" "))
                .map { p => //p is one line
                  Row(
                    if (p(0) == null) null else p(0),
                    if (p(1) == null) null else p(1).toDouble
                    //                 if (p(0) == null) null else p(0),
                    //                 if (p(1) == null) null else p(1).toInt,
                    //                 if (p(2) == null) null else p(2).toInt,
                    //                 if (p(3) == null) null else p(3).toInt,
                    //                 if (p(4) == null) null else p(4).toDouble,
                    //                 if (p(5) == null) null else p(5).toDouble,
                    //                 if (p(6) == null) null else p(6).toDouble,
                    //                 if (p(7) == null) null else p(7).toDouble,
                    //                 if (p(8) == null) null else p(8).toDouble,
                    //                 if (p(9) == null) null else p(9).toDouble,
                    //                 if (p(10) == null) null else p(10).toDouble,
                    //                 if (p(11) == null) null else p(11).toDouble,
                    //                 if (p(12) == null) null else p(12).toDouble,
                    //                 if (p(13) == null) null else p(13).toDouble,
                    //                 if (p(14) == null) null else p(14).toDouble,
                    //                 if (p(15) == null) null else p(15).toDouble,
                    //                 if (p(16) == null) null else p(16).toDouble,
                    //                 if (p(17) == null) null else p(17).toDouble,
                    //                 if (p(18) == null) null else p(18).toDouble,
                    //                 if (p(19) == null) null else p(19).toDouble,
                    //                 if (p(20) == null) null else p(20).toDouble,
                    //                 if (p(21) == null) null else p(21).toDouble,
                    //                 if (p(22) == null) null else p(22).toDouble,
                    //                 if (p(23) == null) null else p(23).toInt,
                    //                 if (p(24) == null) null else p(24).toInt
                  )
                }
              Array_Row

            }

            val RDD_String = list_blocks.flatMap(ele => ele)
            //     RDD_String.collect().foreach(println)
            val tableStruct =
              StructType(Array(
                StructField("star_id", StringType, true),
                //             StructField("ccd_num", IntegerType, true),
                //             StructField("imageid", IntegerType, true),
                //             StructField("zone", IntegerType, true),
                //             StructField("ra", DoubleType, true),
                //             StructField("dec", DoubleType, true),
                StructField("mag", DoubleType, true)
                //             StructField("x_pix", DoubleType, true),
                //             StructField("y_pix", DoubleType, true),
                //             StructField("ra_err", DoubleType, true),
                //             StructField("dec_err", DoubleType, true),
                //             StructField("x", DoubleType, true),
                //             StructField("y", DoubleType, true),
                //             StructField("z", DoubleType, true),
                //             StructField("flux", DoubleType, true),
                //             StructField("flux_err", DoubleType, true),
                //             StructField("normmag", DoubleType, true),
                //             StructField("flag", DoubleType, true),
                //             StructField("background", DoubleType, true),
                //             StructField("threshold", DoubleType, true),
                //             StructField("mag_err", DoubleType, true),
                //             StructField("ellipticity", DoubleType, true),
                //             StructField("class_star", DoubleType, true),
                //             StructField("orig_catid", IntegerType, true),
                //             StructField("timestamp", IntegerType, true)
              ))
            sqlContext.createDataFrame(RDD_String, tableStruct).registerTempTable(block_name)
            sqlContext.sql("SELECT * FROM "+block_name).show()


          }

          else if (star_type=="spaceIdx")
          {
            val block_name=star_name
            //----template table---//

            val stringRDD = sc.fromRedisKV(block_name)
            //          stringRDD.collect().foreach(println)

            val star_value=stringRDD.values

            //          star_value.collect().foreach(println)

            //          star_value.split("\n")


            //            star_value(0)
            //          val array_test=star_value.collect()(0)
            //          array_test.foreach(println)

            val list_blocks = star_value.map { only_one_line =>

              val lines = only_one_line.split("\n")

              val Array_Row = lines.map(line => line.split(" "))
                .map { p => //p is one line
                  Row(
                    if (p(0) == null) null else p(0),
                    if (p(1) == null) null else p(1).toInt,
                    if (p(2) == null) null else p(2).toInt,
                    if (p(3) == null) null else p(3).toInt,
                    if (p(4) == null) null else p(4).toDouble,
                    if (p(5) == null) null else p(5).toDouble,
                    if (p(6) == null) null else p(6).toDouble,
                    if (p(7) == null) null else p(7).toDouble,
                    if (p(8) == null) null else p(8).toDouble,
                    if (p(9) == null) null else p(9).toDouble,
                    if (p(10) == null) null else p(10).toDouble,
                    if (p(11) == null) null else p(11).toDouble,
                    if (p(12) == null) null else p(12).toDouble,
                    if (p(13) == null) null else p(13).toDouble,
                    if (p(14) == null) null else p(14).toDouble,
                    if (p(15) == null) null else p(15).toDouble,
                    if (p(16) == null) null else p(16).toDouble,
                    if (p(17) == null) null else p(17).toDouble,
                    if (p(18) == null) null else p(18).toDouble,
                    if (p(19) == null) null else p(19).toDouble,
                    if (p(20) == null) null else p(20).toDouble,
                    if (p(21) == null) null else p(21).toDouble,
                    if (p(22) == null) null else p(22).toDouble,
                    if (p(23) == null) null else p(23).toInt,
                    if (p(24) == null) null else p(24).toInt,
                    if (p(25) == null) null else p(25).toInt
                  )
                }
              Array_Row

            }

            val RDD_String = list_blocks.flatMap(ele => ele)
            //     RDD_String.collect().foreach(println)
            val tableStruct =
              StructType(Array(
                StructField("star_id", StringType, true),
                StructField("ccd_num", IntegerType, true),
                StructField("imageid", IntegerType, true),
                StructField("zone", IntegerType, true),
                StructField("ra", DoubleType, true),
                StructField("dec", DoubleType, true),
                StructField("mag", DoubleType, true),
                StructField("x_pix", DoubleType, true),
                StructField("y_pix", DoubleType, true),
                StructField("ra_err", DoubleType, true),
                StructField("dec_err", DoubleType, true),
                StructField("x", DoubleType, true),
                StructField("y", DoubleType, true),
                StructField("z", DoubleType, true),
                StructField("flux", DoubleType, true),
                StructField("flux_err", DoubleType, true),
                StructField("normmag", DoubleType, true),
                StructField("flag", DoubleType, true),
                StructField("background", DoubleType, true),
                StructField("threshold", DoubleType, true),
                StructField("mag_err", DoubleType, true),
                StructField("ellipticity", DoubleType, true),
                StructField("class_star", DoubleType, true),
                StructField("orig_catid", IntegerType, true),
                StructField("timestamp", IntegerType, true),
                StructField("block_index", IntegerType, true)
              ))

            val template_name="template_"+ccd_num

//            sqlContext.createDataFrame(RDD_String, tableStruct).cache()

            sqlContext.createDataFrame(RDD_String, tableStruct).registerTempTable(template_name)

//            sqlContext.sql("SELECT * FROM "+template_name).show()


          }

  }

  def readFromRedis_original_table_time_interval(star_name:String,min_time:Int,max_time:Int):Unit=
  {

      val block_name=star_name
      //-----nomal star------//

      val zsetRDD = sc.fromRedisZRangeByScore(block_name, min_time, max_time) //.collect()

      val list_blocks = zsetRDD.map { one_15s_block =>

        val lines = one_15s_block.split("\n")

        val Array_Row = lines.map(line => line.split(" "))
          .map { p => //p is one line
            Row(
              if (p(0) == null) null else p(0),
              if (p(1) == null) null else p(1).toDouble
              //                 if (p(0) == null) null else p(0),
              //                 if (p(1) == null) null else p(1).toInt,
              //                 if (p(2) == null) null else p(2).toInt,
              //                 if (p(3) == null) null else p(3).toInt,
              //                 if (p(4) == null) null else p(4).toDouble,
              //                 if (p(5) == null) null else p(5).toDouble,
              //                 if (p(6) == null) null else p(6).toDouble,
              //                 if (p(7) == null) null else p(7).toDouble,
              //                 if (p(8) == null) null else p(8).toDouble,
              //                 if (p(9) == null) null else p(9).toDouble,
              //                 if (p(10) == null) null else p(10).toDouble,
              //                 if (p(11) == null) null else p(11).toDouble,
              //                 if (p(12) == null) null else p(12).toDouble,
              //                 if (p(13) == null) null else p(13).toDouble,
              //                 if (p(14) == null) null else p(14).toDouble,
              //                 if (p(15) == null) null else p(15).toDouble,
              //                 if (p(16) == null) null else p(16).toDouble,
              //                 if (p(17) == null) null else p(17).toDouble,
              //                 if (p(18) == null) null else p(18).toDouble,
              //                 if (p(19) == null) null else p(19).toDouble,
              //                 if (p(20) == null) null else p(20).toDouble,
              //                 if (p(21) == null) null else p(21).toDouble,
              //                 if (p(22) == null) null else p(22).toDouble,
              //                 if (p(23) == null) null else p(23).toInt,
              //                 if (p(24) == null) null else p(24).toInt
            )
          }
        Array_Row

      }

      val RDD_String = list_blocks.flatMap(ele => ele)
      //     RDD_String.collect().foreach(println)
      val tableStruct =
        StructType(Array(
          StructField("star_id", StringType, true),
          //             StructField("ccd_num", IntegerType, true),
          //             StructField("imageid", IntegerType, true),
          //             StructField("zone", IntegerType, true),
          //             StructField("ra", DoubleType, true),
          //             StructField("dec", DoubleType, true),
          StructField("mag", DoubleType, true)
          //             StructField("x_pix", DoubleType, true),
          //             StructField("y_pix", DoubleType, true),
          //             StructField("ra_err", DoubleType, true),
          //             StructField("dec_err", DoubleType, true),
          //             StructField("x", DoubleType, true),
          //             StructField("y", DoubleType, true),
          //             StructField("z", DoubleType, true),
          //             StructField("flux", DoubleType, true),
          //             StructField("flux_err", DoubleType, true),
          //             StructField("normmag", DoubleType, true),
          //             StructField("flag", DoubleType, true),
          //             StructField("background", DoubleType, true),
          //             StructField("threshold", DoubleType, true),
          //             StructField("mag_err", DoubleType, true),
          //             StructField("ellipticity", DoubleType, true),
          //             StructField("class_star", DoubleType, true),
          //             StructField("orig_catid", IntegerType, true),
          //             StructField("timestamp", IntegerType, true)
        ))
      sqlContext.createDataFrame(RDD_String, tableStruct).registerTempTable(block_name)
      sqlContext.sql("SELECT * FROM "+block_name).show()


  }





}
