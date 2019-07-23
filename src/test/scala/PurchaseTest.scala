
import org.apache.spark.sql.SparkSession
import org.scalatest._


class PurchaseTest extends FunSuite with BeforeAndAfterEach  {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  test("Test_1"){

    assert(ccexam.GeroceryOrders(sparkSession,"input/order_products_test1.csv", "input/products_test1.csv", "output/Test1Output/outputTest1") == "Successful")
  }

  test("Test_2 with one empty field in products_test2.csv"){

    assert(ccexam.GeroceryOrders(sparkSession,"input/order_products_test2.csv", "input/products_test2.csv", "output/Test2Output/outputTest2") == "Unsuccessful")
  }

  test("Test_3 with one missing department id field in products_test3.csv"){

    assert(ccexam.GeroceryOrders(sparkSession,"input/order_products_test2.csv", "input/products_test3.csv", "output/Test3Output/outputTest3") == "Unsuccessful")
  }


}

