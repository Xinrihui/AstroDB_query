import lib.{read_template_table, query_engine_v2}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}


object testing  extends App{

  val conf = new SparkConf()
    .setAppName(s"spark_app_testing")
    .set("spark.scheduler.mode", "FAIR")
    .set("redis.host", "192.168.0.90")
    .set("redis.port", "7001")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[RDD[String]],classOf[RDD[Row]]))
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  //----for testing readFromRedis_Sorted_Sets() ---//
  def unit_testing_2()= {
    val qEngine = new query_engine_v2(sc, sqlContext)
//    qEngine.readFromRedis_basic_time_spatial_index("ab_ref_1_172030")
//    qEngine.readFromRedis_basic_time_spatial_index("block_9_143")



//    qEngine.readFromRedis_basic_time_spatial_index("spaceIdx_5_356")


  }


  def unit_testing_3()={
    val template_table_object=new read_template_table(sc,sqlContext)
    val qEngine = new query_engine_v2(sc, sqlContext)

    val ccd_array=Array(1,2,3,4,5,6,7,8,9)
    template_table_object.readFromRedis("spaceIdx",10,ccd_array)

    sqlContext.sql("SELECT star_id,block_index FROM "+"template_1 "+"where star_id='ref_1_125432' ").show()

    qEngine.readFromRedis_original_table_time_interval("block_9_143",57065,57142)  //the time range is (57065,57142)


  }



  //-- for testing the difference between map() and mapPartitions()--//
 //  http://wanshi.iteye.com/blog/2183906
  def myfuncPerPartition(iter: Iterator[Int]): Iterator[Int] = {
    println("run in partition")
    var res = for (e <- iter) yield {
      val a=1
      (e * 2)+a
    }
    res
  }

  def unit_testing_1()= {
    val a = sc.parallelize(1 to 10, 3)
    def myfuncPerElement(e: Int): Int = {
      println("e=" + e)
      e * 2
    }


    val b = a.map(myfuncPerElement).collect.foreach(println)
    val c = a.mapPartitions(myfuncPerPartition).collect().foreach(println)

  }


//    unit_testing_2()
//  unit_testing_1()
    unit_testing_3()


}
