# MongoSparkConnector
The Standard Mongo Spark Connector provides update but only at the first level of the document. It does not support partial updates/upserts starting at the second level of a BSON document.

This is a MongoSpark Connector that supports partial updates and upsert of Documents in a streaming pipeline at any level of the json includinng arrays. Currently, you can only overwrite an array field with the incoming value. I will subsequently provide an option to update an existing array item too.

Its a memory and cpu intensive connector. This has proven to work best at a minimum of 4g driver and executor memory for a moderately busy stream. Furthermore, this has been tested on Spark 2.4.7 using the forEachBatch or directly on dataframes since it uses implicits.