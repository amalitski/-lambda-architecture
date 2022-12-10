package lambda;

import java.util.List;

public class Serving {
    @Path("/getStreamTags")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getStreamTags() {

        String serverIP = "127.0.0.1";
        String keyspace = "populartweets";
        Cluster cluster = Cluster.builder().addContactPoints(serverIP)
                .build();

        Session session = cluster.connect(keyspace);
        List<String> list = new ArrayList<String>();
        session.execute("select tag,count from stream_tag_count limit 10").                forEach((row) -> {
            String tag = row.get("tag", String.class);
            list.add(tag);
        });
        return list;
    }
    @Path("/getBatchTags")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getBatchTags() {

        String serverIP = "127.0.0.1";
        String keyspace = "populartweets";

        Cluster cluster = Cluster.builder().addContactPoints(serverIP)
                .build();

        Session session = cluster.connect(keyspace);
        List<String> list = new ArrayList<String>();
        session.execute("select tag,count from batch_tag_count limit 10")
                .forEach((row) -> {
                    String tag = row.get("tag", String.class);
                    list.add(tag);
                });
        return list;
    }
}
