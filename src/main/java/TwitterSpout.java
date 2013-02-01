import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream stream;
    private String twitterUsername;
    private String twitterPassword;

    public TwitterSpout(String username, String password) {
        this.twitterUsername = username;
        this.twitterPassword = password;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Provides tuples of the form ["message", "timestamp"]
        //     where   message = the text of a single tweet (String)
        //           timestamp = the time the tweet was received (java.util.Calendar)
        outputFieldsDeclarer.declare(new Fields("message", "timestamp"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        collector = spoutOutputCollector;

        // Create a listener that queues tweets as they arrive and ignores other API notices
        StatusListener twitListener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override public void onTrackLimitationNotice(int i) {}
            @Override public void onScrubGeo(long l, long l2) {}
            @Override public void onStallWarning(StallWarning stallWarning) {}
            @Override public void onException(Exception e) {}
        };

        // Connect and start streaming!
        TwitterStreamFactory factory = new TwitterStreamFactory(
                new ConfigurationBuilder().setUser(this.twitterUsername)
                                          .setPassword(this.twitterPassword)
                                          .build()
        );
        stream = factory.getInstance();
        stream.addListener(twitListener);
        // We're using the Twitter sample stream, which is the most tweets/second you can get without special permission
        // from Twitter
        stream.sample();
    }

    @Override
    public void nextTuple() {
        // Try to pull a tweet off of the queue. If there is none, wait briefly.
        Status nextTwit = queue.poll();
        if (nextTwit != null) {
            collector.emit(new Values(nextTwit.getText(), Calendar.getInstance()));
        } else {
            Utils.sleep(50); // .05 seconds
        }
    }

    @Override
    public void close() {
        stream.shutdown();
    }
}
