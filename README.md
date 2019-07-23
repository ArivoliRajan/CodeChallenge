Solution is implemented using Spark and Scala.
Used Spark 2.3.3
Scala version 2.11.12
SBT version 0.13

Here the output 
use products1.csv from my repo as I replaced the comma and quotes as single space in column 'product_name', 
order_products__prior.csv is used as it is as original file, no changes on this file

1  2236432  1024542  0.46
2  36291  21485  0.59
3  1176787  437599  0.37
4  9479291  3318581  0.35
5  153696  66101  0.43
6  269253  169837  0.63
7  2690129  932237  0.35
8  97724  38964  0.40
9  866627  467046  0.54
10  34573  14623  0.42
11  447123  303539  0.68
12  708931  306489  0.43
13  1875577  1225276  0.65
14  709569  311556  0.44
15  1068058  579523  0.54
16  5414016  1786795  0.33
17  738666  441591  0.60
18  423802  178433  0.42
19  2887550  1229577  0.43
20  1051249  412385  0.39
21  69145  41774  0.60


Here is the output of Test1
3  2  1  0.50
4  2  0  0.00
12  1  0  0.00
13  2  1  0.50
16  2  0  0.00

There is no output for Test2 and Test3 as they are negative usecases
