import org.apache.spark.sql._

import org.apache.spark.sql.SparkSession


object ccexam {


  object CodeChallenge {

    def main(args: Array[String]): Unit = {

      val session = SparkSession.builder()
        .appName("cc")
        .master("local[1]")
        .getOrCreate()
      session.sparkContext.setLogLevel("INFO")

      GeroceryOrders(session)
      val sc = session.sparkContext

    }

    def GeroceryOrders(session: SparkSession) = {
      // val ordersPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/order_products.csv"

      val ordersPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/instacart_2017_05_01/order_products__prior.csv"
      val orders_df = session.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", true)
        .option("mode", "DROPMALFORMED")
        .load(ordersPath)
        .select("product_id", "reordered")
      //.show(90)
      // orders_df.write.csv("/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/ord.csv")


      //val productsPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/products.csv"

      val productsPath = "/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/instacart_2017_05_01/products1.csv"
      val products_df = session.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", true)
        .load(productsPath)
        .select("product_id", "department_id")
      // products_df.show(90000000)
      //  products_df.write.csv("/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/prod_dept.csv")

      val deptReordered3 = orders_df.join(products_df, orders_df("product_id") === products_df("product_id"))
        .select("department_id", "reordered")

      deptReordered3.createOrReplaceTempView("DeptReorderTable")
      //.show(90)

      session.udf.register("firstTime", (ord: Int) => if (ord == 0) 1 else 0)
      val deptAndOrderCount = session.sql("select department_id, count(reordered) orderCount, sum(firstTime(reordered))  FirstTimeOrder from DeptReorderTable  group by department_id ")
      deptAndOrderCount.printSchema()
      deptAndOrderCount.rdd
        .map { case Row(deptid: Int, totalOrder: Long, firsttimeOrder: Long) => (deptid, totalOrder, firsttimeOrder, "%03.2f".format(firsttimeOrder.toDouble / totalOrder.toDouble)) }
        .sortBy(_._1)
        .saveAsTextFile("/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/prod_dept.csv")
      //.write.csv("/Users/kjayakalimuthu/BigdataTrunk/CodeChallenge/prod_dept.csv")
      //.take(30).foreach(println)


    }


  }

}
