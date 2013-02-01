import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class TwitSplitterBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // Split each method into words delimited by non-(alphanumeric or '#') characters
        for (String word : tuple.getString(0).split("[^a-zA-Z0-9#]+")) {
            // For each word, if it's a hashtag, emit it with a timestamp
            if (word.startsWith("#")) {
                collector.emit(new Values(word, tuple.getValue(1)));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Provides tuples of the form ["hashtag", "timestamp"]
        //     where   hashtag = a hashtag (String)
        //           timestamp = the time the tweet in which this hashtag appeared was received (java.util.Calendar)
        outputFieldsDeclarer.declare(new Fields("hashtag", "timestamp"));
    }
}
