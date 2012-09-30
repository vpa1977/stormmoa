package moa.storm.tasks;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;

import javax.xml.bind.DatatypeConverter;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;

import weka.core.Instance;
import weka.core.Utils;
import moa.classifiers.Classifier;
import moa.core.ObjectRepository;
import moa.evaluation.ClassificationPerformanceEvaluator;
import moa.evaluation.LearningEvaluation;
import moa.options.ClassOption;
import moa.options.FileOption;
import moa.options.IntOption;
import moa.options.StringOption;
import moa.streams.InstanceStream;
import moa.tasks.TaskMonitor;

public class TridentEvaluateModel extends BaseEmitTask {

	  @Override
	    public String getPurposeString() {
	        return "Evaluates a static model on a stream.";
	    }

	    private static final long serialVersionUID = 1L;

	    public ClassOption modelOption = new ClassOption("model", 'm',
	            "Classifier to evaluate.", Classifier.class, "LearnModel"); // TridentLearnModel

	    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
	            "Classification performance evaluation method.",
	            ClassificationPerformanceEvaluator.class,
	            "BasicClassificationPerformanceEvaluator");

	    public IntOption maxInstancesOption = new IntOption("maxInstances", 'i',
	            "Maximum number of instances to test.", 1000000, 0,
	            Integer.MAX_VALUE);
	    
//		public StringOption outputPredictionOption = new StringOption("amqpQueue", 'o',
//				"AMQP Exchange", "moa-output");

	
    public Class<?> getTaskResultType() {
        return LearningEvaluation.class;
    }

	@Override
	protected Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {
        
		String topologyName = amqpQueueOption.getValue() + System.currentTimeMillis();
		
		Classifier model = (Classifier) getPreparedClassOption(this.modelOption);
        InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
        ClassificationPerformanceEvaluator evaluator = (ClassificationPerformanceEvaluator) getPreparedClassOption(this.evaluatorOption);
        
		monitor.setCurrentActivity("Connecting to AMQP Broker", 1.0);
		monitor.setCurrentActivityFractionComplete(0);
		LocalDRPC drpc = MoaStormSupport.drpc();
		try {
			connect();
			int maxInstances = this.maxInstancesOption.getValue();
			
			try {
				//        -> Train instances -> State (Classifier)
				//                                    \
				//                  ->Test instances ->- evaluate  instance ->-aggregate (Evaluator)  
				//
				//                                              -> return classification result                   
				// nb: drpc == null - remote mode drpc. 
				StateFactory factory = MoaStormSupport.stateFactory(); 
				TridentTopology topology = new TridentTopology();
				
				
				LearnerWrapper wrapper = new LearnerWrapper( model );
				Stream instanceStream = createStream(amqpQueueOption.getValue(),topology);
				TridentState classifierState = instanceStream.persistentAggregate(factory, new Fields("instance"), 
						new LearnerAggregator(wrapper), new Fields("classifier"));
				
				topology.newDRPCStream("evaluate", drpc).
						stateQuery(classifierState, new Fields("instance"), new EvaluateQueryFunction(), new Fields("prediction"));
				
				Config conf = new Config();
				// conf.setDebug(true);
				//conf.setMaxTaskParallelism(1);
				
				MoaStormSupport.submit(topologyName, conf, topology.build());
			// - topology setup
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}			
			
			// update model
			send(model);
			// update evaluator
			send(evaluator);
			
			
			// attempt to query stuff.
			
		    Instance testInst = (Instance) stream.nextInstance().copy();
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			os.writeObject(testInst);
			byte[] bytes = bos.toByteArray();
			String arg  = DatatypeConverter.printBase64Binary(bytes);
			String ret = drpc.execute("evaluate", arg);
			System.out.println(ret);
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
		
		
        
        
    /*    
        long instancesProcessed = 0;
        monitor.setCurrentActivity("Evaluating model...", -1.0);

        //File for output predictions
        File outputPredictionFile = this.outputPredictionFileOption.getFile();
        PrintStream outputPredictionResultStream = null;
        if (outputPredictionFile != null) {
            try {
                if (outputPredictionFile.exists()) {
                    outputPredictionResultStream = new PrintStream(
                            new FileOutputStream(outputPredictionFile, true), true);
                } else {
                    outputPredictionResultStream = new PrintStream(
                            new FileOutputStream(outputPredictionFile), true);
                }
            } catch (Exception ex) {
                throw new RuntimeException(
                        "Unable to open prediction result file: " + outputPredictionFile, ex);
            }
        }
        while (stream.hasMoreInstances()
                && ((maxInstances < 0) || (instancesProcessed < maxInstances))) {
            Instance testInst = (Instance) stream.nextInstance().copy();
            int trueClass = (int) testInst.classValue();
            //testInst.setClassMissing();
            double[] prediction = model.getVotesForInstance(testInst);
            //evaluator.addClassificationAttempt(trueClass, prediction, testInst
            //		.weight());
            if (outputPredictionFile != null) {
                outputPredictionResultStream.println(Utils.maxIndex(prediction) + "," + trueClass);
            }
            evaluator.addResult(testInst, prediction);
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
                    monitor.setLatestResultPreview(new LearningEvaluation(
                            evaluator.getPerformanceMeasurements()));
                }
            }
        }
        if (outputPredictionResultStream != null) {
            outputPredictionResultStream.close();
        }*/
		
        return new LearningEvaluation(evaluator.getPerformanceMeasurements());	
      }

}
