# Spark SQL

[Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) is a Spark module for structured data processing. 
Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. 

Internally, Spark SQL uses this extra information to perform extra optimizations. There are several ways to interact with Spark SQL including SQL and the Dataset API. 
When computing a result, the same execution engine is used, independent of which API/language you are using to express the computation. 
This unification means that developers can easily switch back and forth between different APIs based on which provides the most natural way to express a given transformation.
