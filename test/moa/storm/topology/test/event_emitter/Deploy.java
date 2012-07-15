package moa.storm.topology.test.event_emitter;

import java.util.Map;

import weka.core.Instance;

import moa.storm.scheme.InstanceScheme;

import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.spout.AMQPSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Deploy {
	 public static class ExclamationBolt extends BaseRichBolt {
	        OutputCollector _collector;

	        @Override
	        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	            _collector = collector;
	        }

	        @Override
	        public void execute(Tuple tuple) {
	        	Instance inst = (Instance)tuple.getValue(0);
	        	System.out.println(inst);
	        	
	            _collector.ack(tuple);
	        }

	        @Override
	        public void declareOutputFields(OutputFieldsDeclarer declarer) {
	            declarer.declare(new Fields("word"));
	        }


	    }
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
			//System.setProperty("storm.jar", "/home/bsp/stormmoa.jar");
	        TopologyBuilder builder = new TopologyBuilder();
	        
	        
	        SharedQueueWithBinding queue = new SharedQueueWithBinding("moa-events", "moa", "#");
	        String host = "localhost";
	        int port = 5672; // default ampq host
	        String vhost = "/";
	        String username = "guest";
	        String password = "test";
	        AMQPSpout spout = new AMQPSpout(host, port, username, password, vhost, queue, new InstanceScheme());
	        builder.setSpout("instances", spout);
	        // consume instances
	        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
	                .shuffleGrouping("instances");
	                
	        Config conf = new Config();
	        conf.setDebug(true);
/*			if(args!=null && args.length > 0) {
	            conf.setNumWorkers(3);
	            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	        } 
	        else 
*/	        
	        {
	            conf.setMaxTaskParallelism(3);
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology("word-count", conf, builder.createTopology());
	        }
	}
}
