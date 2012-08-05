package moa.storm.tasks;

import java.util.ArrayList;
import java.util.Map;

import moa.classifiers.Classifier;
import moa.options.ClassOption;
import moa.options.IntOption;
import moa.storm.scheme.InstanceScheme;
import moa.streams.InstanceStream;
import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.spout.AMQPSpout;

public class LearnModelTopology extends TopologyFragment {

    @Override
    public String getPurposeString() {
        return "Learns a model from a stream.";
    }

    private static final long serialVersionUID = 1L;

    public ClassOption learnerOption = new ClassOption("learner", 'l',
            "Classifier to train.", Classifier.class, "bayes.NaiveBayes");

    public ClassOption streamOption = new ClassOption("stream", 's',
            "Stream to learn from.", InstanceStream.class,
            "generators.RandomTreeGenerator");

    public IntOption maxInstancesOption = new IntOption("maxInstances", 'm',
            "Maximum number of instances to train on per pass over the data.",
            10000000, 0, Integer.MAX_VALUE);

    public IntOption memCheckFrequencyOption = new IntOption(
            "memCheckFrequency", 'q',
            "How many instances between memory bound checks.", 100000, 0,
            Integer.MAX_VALUE);
    
    

    public LearnModelTopology() {
    	
    }

    public LearnModelTopology(Classifier learner, InstanceStream stream,
            int maxInstances) {
        this.learnerOption.setCurrentObject(learner);
        this.streamOption.setCurrentObject(stream);
        this.maxInstancesOption.setValue(maxInstances);
    }
    
    private class LearningBolt extends BaseRichBolt implements IRichBolt 
    {
    	/**
		 * 
		 */
		private static final long serialVersionUID = 2134063986825105707L;
		private transient OutputCollector outputCollector;
    	private Fields outputFields = new Fields("learned_instance", "monitor", "classifier");
    	private int numInstances;
    	private int processedInstances;
    	private Classifier classifier;
    	

    	public LearningBolt(Classifier cls, int ni)
    	{
    		this.classifier = cls;
    		this.numInstances = ni;
    	}
    	
    	
		@SuppressWarnings("rawtypes")
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			outputCollector = collector;
			processedInstances = 0;
			
		}

		
		public void execute(Tuple input) {
			String stream = input.getSourceStreamId();
			if ("instance".equals(stream)) 
			{
				Instance inst = (Instance)input.getValue(0);
				
				classifier.trainOnInstance(inst);
				if (processedInstances++ == numInstances)
				{
					ArrayList<Object> list = new ArrayList<Object>();
					list.add( classifier);
					outputCollector.emit("classifier", list);
				}
				//echo 
				outputCollector.emit("learned_instance", input.getValues());
			}
		}

		
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(outputFields);
		}

    	
    }


	@Override
	public TopologyBuilder build(TopologyBuilder topologyBuilder) {
        Classifier learner = (Classifier) getPreparedClassOption(this.learnerOption);
        InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
        learner.setModelContext(stream.getHeader());
        
        SharedQueueWithBinding queue = new SharedQueueWithBinding("moa-events", "moa", "#");
        String host = "localhost";
        int port = 5672; // default ampq host
        String vhost = "/";
        String username = "guest";
        String password = "test";
        AMQPSpout spout = new AMQPSpout(host, port, username, password, vhost, queue, new InstanceScheme());
        topologyBuilder.setSpout("instances", spout);
        topologyBuilder.setBolt("learner", new LearningBolt(learner, maxInstancesOption.getValue())).shuffleGrouping("instance");
        return topologyBuilder;
	}
}
