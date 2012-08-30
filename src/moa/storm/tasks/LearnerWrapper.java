package moa.storm.tasks;

import java.io.Serializable;

import moa.classifiers.Classifier;

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
	}
	public long getInstancesProcessed() {
		return instancesProcessed;
	}

	public void increaseInstancesProcessed()
	{
		instancesProcessed++;
	}
	private Classifier classifier;
	private long instancesProcessed;
}
