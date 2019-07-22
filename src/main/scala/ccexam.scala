import org.apache.spark.sql.SparkSession

object ccexam {
  def main(args: Array[String]):Unit = {
    val session  = SparkSession.builder()
      .appName("cc")
      .master(args(0))
      .getOrCreate()
    //session.sparkContext.setLogLevel("INFO")

    val path1 = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/order_products.csv"
    val orders_df = session.read
      .format("csv")
      .option("header","true")
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
      .load(path1)
      .select("product_id", "reordered")
    orders_df.show(90)
    // orders_df.write.csv(args(2))
    session.stop()

  }

}
