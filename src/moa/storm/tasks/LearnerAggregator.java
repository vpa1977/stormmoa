package moa.storm.tasks;

import moa.classifiers.Classifier;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

class LearnerAggregator implements ReducerAggregator<LearnerWrapper> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private LearnerWrapper learner;
	
	public LearnerAggregator( LearnerWrapper c)
	{
		learner = c;
	}

	public LearnerWrapper init() {
		return learner;
	}

	public LearnerWrapper reduce(LearnerWrapper curr, TridentTuple tuple) {
		if (curr == null)
			curr = init();
		Instance trainInst = (Instance) tuple.getValue(0);
		curr.getClassifier().trainOnInstance(trainInst);
		curr.increaseInstancesProcessed();
		return learner;
	}
}