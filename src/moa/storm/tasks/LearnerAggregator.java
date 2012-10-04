package moa.storm.tasks;

import moa.classifiers.Classifier;
import moa.evaluation.ClassificationPerformanceEvaluator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

public class LearnerAggregator implements ReducerAggregator<LearnerWrapper> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private LearnerWrapper learner;
	
	/** 
	 * 
	 * @param c - wrapper for the classifier/evaluator pair
	 */
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
		Object value = tuple.getValue(0);
		if (value instanceof Instance){
			Instance trainInst = (Instance) value;
			curr.getClassifier().trainOnInstance(trainInst);
			curr.increaseInstancesProcessed();
		}
		else 
		if (value instanceof Classifier)
		{
			curr.setClassifier((Classifier)value);
		}
		else
		if (value instanceof ClassificationPerformanceEvaluator)
		{
			curr.setEvaluator( (ClassificationPerformanceEvaluator)value );
		}
		else
		{
			throw new RuntimeException("Unknown tuple type");
		}
		return learner;
	}
}