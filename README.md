# Spark Challenge

## Stack

- Spark 2.2.1
- Scala 2.11.6
- OpenJDK 1.8.0_151
- Tested on Ubuntu 16.04.3

## Comments

- RDD operations are lazy by default. `cache` persists all operations so far on top of a RDD to memory,
so that further new actions on the dataset do not trigger the same operations again. It's more or less
memoization - can be used to avoid doing expensive transformations or loading data repeatedly from disk.
- Spark is usually faster than MapReduce due to using more caching and due to the DAG-based execution engine,
where data operations are lazy and can be optimized as a whole before being executed.
- Spark Contexts are entrypoints for the Spark core API and represent a connection to a Spark cluster.
- Resilient​ ​Distributed​ Datasets are a abstraction over the raw storage and handling of data by the Spark cluster,
such that they process everything in parallel, trying to minimize IO by partitioning the raw data and making nodes
prioritize reads to other closer nodes, and with in-memory processing such that RDD can be effectively immutable,
and operations on then do not have side-effects on the backing store.
- `groupByKey` requires constructing a Iterable with all values for each key, where `reduceByKey`
does not, which improves performance
- This is a word count example:
```scala
    // Load a textfile as a RDD of lines from Hadoop File System (HDFS)
    val textFile = sc.textFile("hdfs://..")

    val counts = textFile
      .flatMap(line => line.split(" ")) // split all lines into words and flatten the entire result
      .map(word => (word, 1)) // map to a key-value tuple, with words as keys and a value of 1
      .reduceByKey(_ + _) // group by key and execute a reduce operation on all the grouped values
    
    // Serialize the final RDD to a text file stored in a HDFS deployment
    counts.saveAsTextFile("hdfs://...")
```
