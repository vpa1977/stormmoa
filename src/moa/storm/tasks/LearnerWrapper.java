package moa.storm.tasks;

import java.io.Serializable;

import moa.classifiers.Classifier;
import moa.evaluation.ClassificationPerformanceEvaluator;
import weka.core.Instance;

public class LearnerWrapper implements Serializable{
	
	public LearnerWrapper(Classifier c)
	{
		instancesProcessed = 0;
		setClassifier(c);
	}
	public Classifier getClassifier() {
		return classifier;
	}
	public void setClassifier(Classifier classifier) {
		this.classifier = classifier;
		this.instancesProcessed = 0;
	}
	public long getInstancesProcessed() {
		return instancesProcessed;
	}

	public void increaseInstancesProcessed()
	{
		instancesProcessed++;
	}
	public void setEvaluator(ClassificationPerformanceEvaluator value) {
		this.evaluator = value;
	}
	
	private Classifier classifier;
	
	
	private ClassificationPerformanceEvaluator evaluator;
	
	private long instancesProcessed;

	public void addEvaluation(Object prediction, Instance inst) {
		evaluator.addResult(inst, (double[])prediction);
	}
	
	
}
