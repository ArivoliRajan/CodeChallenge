import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object ccexam {


    def main(args: Array[String]): Unit = {

      if (args.size < 4) {
        new Exception("Expected 4 input parameters: got less than 4 parameters. Syntax : sbt run 'serverMode  ordersPath productsPath  outputpath '")
      }

      val session = SparkSession.builder()
        .appName("cc")
        .master(args(0))
        .getOrCreate()
      session.sparkContext.setLogLevel("INFO")

      GeroceryOrders(session, args(1), args(2), args(3))

    }

    def GeroceryOrders(session: SparkSession, ordersPath : String, productsPath : String,  outputpath : String) = {
      try{
        // val ordersPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/order_products.csv"
        //val ordersPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/instacart_2017_05_01/order_products__prior.csv"
        val orders_df = session.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", true)
          .load(ordersPath)
          .select("product_id", "reordered")


        //val productsPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/products.csv"
        // val productsPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/instacart_2017_05_01/products1.csv"
        val products_df = session.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", true)
          .load(productsPath)
          .select("product_id", "department_id")

        //Join Orders and Products to retrieve department and Reordered columns
        val deptReordered3 = orders_df.join(products_df, orders_df("product_id") === products_df("product_id"))
          .select("department_id", "reordered")

        deptReordered3.createOrReplaceTempView("DeptReorderTable")

        //Create an UDF to calculate the firstTime orders
        session.udf.register("firstTime", (ord: Int) => if (ord == 0) 1 else 0)


        val deptAndOrderCount = session.sql("select department_id, count(reordered) orderCount, sum(firstTime(reordered))  FirstTimeOrder from DeptReorderTable  group by department_id ")
        /*val reportDS = deptAndOrderCount
          .map { case Row(deptid: Int, totalOrder: Long, firsttimeOrder: Long) => (deptid, totalOrder, firsttimeOrder, "%03.2f".format(firsttimeOrder.toDouble / totalOrder.toDouble)) }
          .sort("_1")
          .repartition(1)

        reportDS.write.csv("/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/report.csv")*/

        //Sort by Department ID
        deptAndOrderCount
          .repartition(1)
          .rdd
          .map { case Row(deptid: Int, totalOrder: Long, firsttimeOrder: Long) => (deptid, totalOrder, firsttimeOrder, "%03.2f".format(firsttimeOrder.toDouble / totalOrder.toDouble)) }
          .sortBy(_._1)
          .map(r => r._1 + "  " + r._2 + "  " + r._3 + "  " + r._4)
          .saveAsTextFile(outputpath)

        "Successful"

      }catch {
        case e : Exception => e.printStackTrace()
          "Unsuccessful"

      } finally {
        session.stop()
      }

    }

}
