import org.apache.spark.{SparkConf,SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

object invertedIndexTask {
  def main(args: Array[String]): Unit = {

    val input = this.getClass.getResource("/") + "data"

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //1.获取hadoop操作文件的api
    val fs = FileSystem.get(sc.hadoopConfiguration)
    //2.读取目录下的文件，并生成列表
    val filelist = fs.listFiles(new Path(input), true)
    //3.遍历文件，并读取文件类容成成rdd，结构为（文件名，单词）
    var unionrdd = sc.emptyRDD[(String,String)] // rdd声明变量为 var
    while (filelist.hasNext){
      val abs_path = new Path(filelist.next().getPath.toString)
      val file_name = abs_path.getName //文件名称
      val rdd1 = sc.textFile(abs_path.toString).flatMap(_.split(" ").map((file_name,_)))
      //4.将遍历的多个rdd拼接成1个Rdd
      unionrdd = unionrdd.union(rdd1)
    }
    //5.构建词频（（文件名，单词），词频）
    val rdd2 = unionrdd.map(word => {(word, 1)}).reduceByKey(_ + _)
    //6.//调整输出格式,将（文件名，单词），词频）==》 （单词，（文件名，词频）） ==》 （单词，（文件名，词频））汇总
    val frdd1 = rdd2.map(word =>{(word._1._2,String.format("(%s,%s)",word._1._1,word._2.toString))})
    val frdd2 = frdd1.reduceByKey(_ +"," + _)
    val frdd3 = frdd2.map(word =>String.format("\"%s\",{%s}",word._1,word._2))
    frdd3.foreach(println)
  }

}