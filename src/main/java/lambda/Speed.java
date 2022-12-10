package lambda;

public class Speed {
    public void run(){
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                Durations.minutes(60));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);

// Create stream of tags
        JavaDStream<String> hashTagStream = stream.flatMap((status) -> {
            String textArr[] = status.getText().split(" ");
            List<String> matching = new ArrayList<String>();
            for (String str : textArr) {
                if (str.startsWith("#"))
                    matching.add(str);
            }
            return matching;
        });

// create tag,frequency stream
        JavaPairDStream<String, Integer> tagCountStream = hashTagStream
                .transformToPair((rdd) -> Business.countTags(rdd));

// Create real time view of tag,frequency for each 60 minute duration
        JavaDStream<Tuple2<Integer, String>> countTagSortedStream = tagCountStream
                .reduceByKeyAndWindow(((v1,v2) -> v1 + v2), Durations.minutes(60))
                .map(v1 -> new Tuple2<Integer,String>(v1._2, v1._1))
                .transform((v1) -> v1.sortBy((v2 -> (v2._1)),false,1));

// Save this real time view in cassandra
        DStreamJavaFunctions<Tuple2<Integer, String>> javaF =
                CassandraStreamingJavaUtil.javaFunctions(countTagSortedStream);

        javaF.writerBuilder("populartweets", "stream_tag_count",
                        CassandraJavaUtil.mapTupleToRow(Integer.class,String.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("count", "tag"))
                .saveToCassandra();
    }
}
