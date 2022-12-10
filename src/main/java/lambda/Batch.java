package lambda;

public class Batch {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("TwitterPopularTagsBatch";
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // Load data file
        JavaRDD<String> tags = sc.textFile(args[0]);
        // find (tag, #times)
        JavaPairRDD<String, Integer> tagCounts = Business.countTags(tags);
        // save this batch view to Cassandra
        RDDJavaFunctions<Tuple2<String,Integer>> rddJavaFunctions =
                CassandraJavaUtil.javaFunctions(tagCounts);
        rddJavaFunctions.writerBuilder("populartweets", "batch_tag_count",
                        CassandraJavaUtil.mapTupleToRow(String.class, Integer.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("tag","count"))
                .saveToCassandra();
    }
}
