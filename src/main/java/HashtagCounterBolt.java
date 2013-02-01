import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.*;

public class HashtagCounterBolt extends BaseRichBolt {
    private final Map<String, List<Calendar>> hashtags = new HashMap<String, List<Calendar>>();
    private Timer reportTimer;
    private Jedis redisConnection;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // Connect to redis
        redisConnection = new Jedis("localhost");

        // Prune the internal data structures and report stats every 2 minutes
        reportTimer = new Timer();
        reportTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                pruneAndReport();
            }
        }, 2 * 60 * 1000, 2 * 60 * 1000);
    }

    @Override
    public void cleanup() {
        reportTimer.cancel();
        redisConnection.disconnect();
    }

    @Override
    public void execute(Tuple tuple) {
        String tag = tuple.getString(0);
        List<Calendar> occurrences;

        // Filter out degenerate hashtags and ignore case
        if (tag.equals("#")) return;
        tag = tag.toLowerCase();

        synchronized (hashtags) {
            if (hashtags.containsKey(tag)) {
                occurrences = hashtags.get(tag);
            } else {
                occurrences = new ArrayList<Calendar>();
                hashtags.put(tag, occurrences);
            }
            occurrences.add((Calendar)tuple.getValue(1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields());
    }

    private void pruneAndReport() {
        Calendar threshold = Calendar.getInstance();
        threshold.add(Calendar.MINUTE, -10);

        synchronized (hashtags) {
            Iterator<Map.Entry<String,List<Calendar>>> hashIt = hashtags.entrySet().iterator();
            while (hashIt.hasNext()) {
                Map.Entry<String,List<Calendar>> hashEntry = hashIt.next();

                // Report
                String hashtag = hashEntry.getKey();
                int freq = hashEntry.getValue().size();
                redisConnection.zadd("hashtags", freq, hashtag);

                // Prune
                // Remove any dates prior to
                Iterator<Calendar> instIt = hashEntry.getValue().iterator();
                while (instIt.hasNext()) {
                    Calendar instance = instIt.next();
                    if (instance.before(threshold)) {
                        instIt.remove();
                    }
                }

                if (hashEntry.getValue().isEmpty()) {
                    hashIt.remove();
                }
            }
        }
    }
}
