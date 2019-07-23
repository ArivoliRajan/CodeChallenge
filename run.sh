#Run the below command from the directory which has build.sbt and src directory
#use products1.csv from my repo as I replaced the comma and quotes as single space in column 'product_name'
# 'sbt package' will create a jar file under target directory, use this jar file to run the spark job using spark-submit command

sbt compile
sbt "run local  input/order_products__prior.csv  input/products1.csv  output/report.csv"
sbt package  

spark-submit --class ccexam <path of jar file>/test6_2.11-0.1.jar local  input/order_products__prior.csv  input/products1.csv  output/report.csv
