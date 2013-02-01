import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RecentTrendTopology {

    // Constructs the Twitter trend topology
    private static StormTopology buildTopology() throws Exception {
        // Get the twitter auth details
        String username = System.getProperty("twitter.username");
        String password = System.getProperty("twitter.password");

        if (username == null || password ==  null) {
            throw new Exception("Must specify both twitter.username and twitter.password system properties.");
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-sample", new TwitterSpout(username, password), 1);
        builder.setBolt("splitter", new TwitSplitterBolt(), 2)
               .shuffleGrouping("twitter-sample");
        builder.setBolt("counter", new HashtagCounterBolt(), 2)
               .fieldsGrouping("splitter", new Fields("hashtag"));

        return builder.createTopology();
    }

    // Configuration details live here
    private static Config buildConfiguration() {
        Config config = new Config();

        config.setDebug(true);
        config.setMaxTaskParallelism(4);

        return  config;
    }

    public static void main(String[] args) throws Exception {
        final LocalCluster cluster = new LocalCluster();

        // Regsiter a hook for JVM termination (such as through ^C) in which we gracefully shut down the cluster.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() { cluster.shutdown(); }
        });

        // Start it up!
        cluster.submitTopology("twitter-trends", buildConfiguration(), buildTopology());
    }
}
