import  org.apache.spark.sql.SparkSession 


//Create Spark Session 

val spark =  SparkSession.builder()
             .appName("Reading Data") 
             .getOrCreate()


//Read a users.csv file usieng spark 
val mydataframe  = spark.read 
                  .format("csv")
                  .option("header", "true") 
                  .load("data/users.csv")  


// Print the DataFrame
mydataframe.show()

// Print the top 5 rows
mydataframe.head(5).foreach(println)

// Print the last 5 rows
mydataframe.takeRight(5).foreach(println)

// Print the shape (number of rows and columns)
val numRows = mydataframe.count()
val numCols = mydataframe.columns.length
println(s"Shape: ($numRows, $numCols)")

// Provide descriptive statistics
mydataframe.describe().show()


