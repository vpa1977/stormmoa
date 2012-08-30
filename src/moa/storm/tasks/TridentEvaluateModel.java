package moa.storm.tasks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import weka.core.Instance;
import weka.core.Utils;
import moa.classifiers.Classifier;
import moa.core.ObjectRepository;
import moa.evaluation.ClassificationPerformanceEvaluator;
import moa.evaluation.LearningEvaluation;
import moa.options.ClassOption;
import moa.options.FileOption;
import moa.options.IntOption;
import moa.streams.InstanceStream;
import moa.tasks.TaskMonitor;

public class TridentEvaluateModel extends BaseEmitTask {

	  @Override
	    public String getPurposeString() {
	        return "Evaluates a static model on a stream.";
	    }

	    private static final long serialVersionUID = 1L;

	    public ClassOption modelOption = new ClassOption("model", 'm',
	            "Classifier to evaluate.", Classifier.class, "TridentLearnModel");

	    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
	            "Classification performance evaluation method.",
	            ClassificationPerformanceEvaluator.class,
	            "BasicClassificationPerformanceEvaluator");

	    public IntOption maxInstancesOption = new IntOption("maxInstances", 'i',
	            "Maximum number of instances to test.", 1000000, 0,
	            Integer.MAX_VALUE);

	    public FileOption outputPredictionFileOption = new FileOption("outputPredictionFile", 'o',
	            "File to append output predictions to.", null, "pred", true);
	    
	
	
    public Class<?> getTaskResultType() {
        return LearningEvaluation.class;
    }

	@Override
	protected Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {
        
		
		Classifier model = (Classifier) getPreparedClassOption(this.modelOption);
        InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
        ClassificationPerformanceEvaluator evaluator = (ClassificationPerformanceEvaluator) getPreparedClassOption(this.evaluatorOption);
        
        int maxInstances = this.maxInstancesOption.getValue();
        
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
        }
        return new LearningEvaluation(evaluator.getPerformanceMeasurements());	}

}
