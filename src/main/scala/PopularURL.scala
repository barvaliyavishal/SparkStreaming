import java.util.regex.Matcher

import Helper._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PopularURL {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)


    val ssc = new StreamingContext("local[*]","PopularURL",Seconds(1))

    val pattern = apacheLogPattern()

    val lines = ssc.socketTextStream("127.0.0.1",9999,StorageLevel.MEMORY_AND_DISK_SER)

    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if(matcher.matches())
        matcher.group(5)})

    val urls = requests.map(x => {
      val arr = x.toString.split(" ")
      if(arr.size == 3)
        arr(1)
      else
        "[ERROR]"
    })

    val urlcounts = urls.map(x => (x,1)).reduceByKeyAndWindow(_+_, _-_,Seconds(300),Seconds(1))
    val SortedUrlsCounts = urlcounts.transform(rdd => rdd.sortBy(x => x._2,ascending = false))

    SortedUrlsCounts.print()

    ssc.checkpoint("C:\\A sparkscala\\checkpoint")
    ssc.start()
    ssc.awaitTermination()

  }

}
