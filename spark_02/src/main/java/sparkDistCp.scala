import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FsShell

object sparkDistCp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000")

    val srcPath = args(0)
    val targetPath = args(1)

    var fsShell: FsShell = null

    try {
      fsShell = new FsShell(conf)
      fsShell.run(Array("-rm", "-r", targetPath))
      fsShell.run(Array("-mkdir", "-p", targetPath))
      val code = fsShell.run(Array("-cp", srcPath + "/*", targetPath + "/"))
      println(s"Copy $srcPath to $targetPath ${if (code == 0) "success" else "failed"}.")
    } catch {
      case e: Exception =>
        println(s"Copy $srcPath to $targetPath failed")
        e.printStackTrace()
    } finally {
      if (null != fsShell) {
        fsShell.close()
      }
    }

  }

}

