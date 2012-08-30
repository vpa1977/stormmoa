package moa.storm.tasks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import moa.classifiers.Classifier;
import moa.core.ObjectRepository;
import moa.options.ClassOption;
import moa.options.IntOption;
import moa.storm.tasks.EventEmitter.MOAQueryFunction;
import moa.streams.InstanceStream;
import moa.tasks.TaskMonitor;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import trident.memcached.MemcachedState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.tuple.Fields;

public class TridentLearnModel extends BaseEmitTask {
	
	  @Override
	    public String getPurposeString() {
	        return "Learns a model from a stream.";
	    }

	    private static final long serialVersionUID = 1L;

	    public ClassOption learnerOption = new ClassOption("learner", 'l',
	            "Classifier to train.", Classifier.class, "bayes.NaiveBayes");


	    public IntOption maxInstancesOption = new IntOption("maxInstances", 'm',
	            "Maximum number of instances to train on per pass over the data.",
	            1000, 0, Integer.MAX_VALUE);

	    public IntOption numPassesOption = new IntOption("numPasses", 'p',
	            "The number of passes to do over the data.", 1, 1,
	            Integer.MAX_VALUE);

	    public IntOption memCheckFrequencyOption = new IntOption(
	            "memCheckFrequency", 'q',
	            "How many instances between memory bound checks.", 100000, 0,
	            Integer.MAX_VALUE);
	
	
	private TridentState createLearner(StateFactory factory, Classifier learner, Stream instanceStream)
	{
		return instanceStream.persistentAggregate(factory, new Fields("instance"), 
				new LearnerAggregator(new LearnerWrapper(learner)), new Fields("classifier"));
	}
	
	public Class<?> getTaskResultType() {
		return Classifier.class;
	}

	@Override
	protected Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {
		// connect to the
		String topologyName = "moa_learn ";
		try {
			monitor.setCurrentActivity("Connecting to AMQP Broker", 1.0);
			monitor.setCurrentActivityFractionComplete(0);
			
			connect();
			LocalDRPC drpc = LocalStormSupport.drpc();
			LocalCluster cluster = LocalStormSupport.localCluster();
			
			
			Classifier learner = (Classifier) getPreparedClassOption(this.learnerOption);
			InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
			learner.setModelContext(stream.getHeader());
			
			// + topology setup
				StateFactory factory = LocalStormSupport.stateFactory(); 
				TridentTopology topology = new TridentTopology();
				Stream instanceStream = createStream(topology);
				TridentState classifierState = createLearner(factory, learner, instanceStream);
				Stream queryStream = topology.newDRPCStream("classifier", drpc);
				queryStream.stateQuery(classifierState, new ClassifierQueryFunction(), new Fields("classifier"));
				Stream statStream = topology.newDRPCStream("stats", drpc);
				statStream.stateQuery(classifierState,  new StatQueryFunction(), new Fields("statistics"));
		
				
				Config conf = new Config();
				// conf.setDebug(true);
				conf.setMaxTaskParallelism(1);
				cluster.submitTopology(topologyName, conf, topology.build());
			// - topology setup
			
			
			
			// +send instances into queue
	        int numPasses = this.numPassesOption.getValue();
	        int maxInstances = this.maxInstancesOption.getValue();
	        for (int pass = 0; pass < numPasses; pass++) {
	            long instancesProcessed = 0;
	            monitor.setCurrentActivity("Training learner"
	                    + (numPasses > 1 ? (" (pass " + (pass + 1) + "/"
	                    + numPasses + ")") : "") + "...", -1.0);
	            if (pass > 0) {
	                stream.restart();
	            }
	            while (stream.hasMoreInstances()
	                    && ((maxInstances < 0) || (instancesProcessed < maxInstances))) {
	            	
	            	send(stream.nextInstance());
	                
	                
	                instancesProcessed++;
	                if (instancesProcessed % INSTANCES_BETWEEN_MONITOR_UPDATES == 0) {
	                    if (monitor.taskShouldAbort()) {
	                        return null;
	                    }
	                    long estimatedRemainingInstances = stream.estimatedRemainingInstances();
	                    if (maxInstances > 0) {
	                        long maxRemaining = maxInstances - instancesProcessed;
	                        if ((estimatedRemainingInstances < 0)
	                                || (maxRemaining < estimatedRemainingInstances)) {
	                            estimatedRemainingInstances = maxRemaining;
	                        }
	                    }
	                    monitor.setCurrentActivityFractionComplete(estimatedRemainingInstances < 0 ? -1.0
	                            : (double) instancesProcessed
	                            / (double) (instancesProcessed + estimatedRemainingInstances));
	                    if (monitor.resultPreviewRequested()) {
	                        monitor.setLatestResultPreview(learner.copy());
	                    }
	                }
	            }
	            
		        monitor.setCurrentActivity("Waiting for cluster to finush processing", 1.0);
				monitor.setCurrentActivityFractionComplete(0);
				waitCluster(monitor, drpc, instancesProcessed);
	            
	            
	        }
	        // - send instances into queue
	        monitor.setCurrentActivity("Retrieving classifier", 1.0);
	        
	        // + query number of instances processed
	        // - query number of instances processed
	        
	        // query the classifer
	        String r = drpc.execute("classifier", "");
	        // skip to the data
	        r = r.substring(6);
	        byte[] b = DatatypeConverter.parseBase64Binary(r);
	        ObjectInputStream is = new ObjectInputStream( new ByteArrayInputStream(b));
	        
	        Classifier cls = (Classifier) is.readObject();
	        System.out.println(cls);
			return cls;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			// @TODO reconfigure with new classifier.
			LocalStormSupport.localCluster().killTopology(topologyName);
		}
		return null;
	}

	protected long waitCluster(TaskMonitor monitor, LocalDRPC drpc,
			long instancesProcessed) {
		long clusterProcessed = 0;
		while (clusterProcessed < instancesProcessed )
		{
			String r = drpc.execute("stats", "");
			r = r.substring(5);
			r = r.substring(0, r.length()-2);
			clusterProcessed = Long.parseLong(r);
		    monitor.setCurrentActivityFractionComplete((double) clusterProcessed/(double) (instancesProcessed));
		    try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return clusterProcessed;
	}
	
}
