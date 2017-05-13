import lib.{query_engine, read_template_table}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object astroDB_query {

  def main(args: Array[String]): Unit = {

    if (args(0) == "-h") {
      println("parameter 0: the path of template table")
      println("parameter 1: output path")
      println("parameter 2: the path of star table")
      println("parameter 3: the num of slices")
      println("parameter 4: query name")
      println("parameter 5: query parameters")
      println("parameter 6: the storage backet num")
      println("parameter 7: redis host:port")
      System.exit(1)
    }

    var tempTablePath = new String
    var outputPath = new String
    var starTablePath = new String
    var sliceNum = 0
    var queryName = new String
    var queryParam = new String
    var starClusterNum = 0
    var fromSource = new String   // used for testing (data source, redis or HDFS)
    var redisHost = new String


    for(a <- args.indices)
      {
        if (args(a).head == '-')
          {
            args(a) match {
              case "-tempTablePath" => tempTablePath = args(a+1)
              case "-outputPath"    => outputPath = args(a+1)
              case "-starTablePath"    => starTablePath = args(a+1)
              case "-sliceNum"  => sliceNum = args(a+1).toInt
              case "-queryName" => queryName = args(a+1)
              case "-queryParam" => queryParam = args(a+1)
              case "-starClusterNum" => starClusterNum = args(a+1).toInt // 和哈希模函数相关 (星id % starClusterNum)= 星表簇号
              case "-fromSource" => fromSource = args(a+1)
              case "-redisHost" => redisHost = args(a+1)
              case _ => sys.error(s"${args(a)} is illegal")
            }
          }
      }

    val redisIP = redisHost.split(":")(0)
    val redisPort = redisHost.split(":")(1)

    val conf = new SparkConf()
      .setAppName(s"AstroDB_query_BacketNum_$starClusterNum")
      .set("spark.scheduler.mode", "FAIR")
      .set("redis.host", redisIP)
      .set("redis.port", redisPort)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[RDD[String]],classOf[RDD[Row]]))
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    new read_template_table(sc,sqlContext).readFromLocal(tempTablePath,sliceNum)
    val qEngine = new query_engine(sc, sqlContext)
    qEngine.runUserQuery(queryName, queryParam,sliceNum,outputPath,fromSource,starTablePath,starClusterNum)



 }
}
