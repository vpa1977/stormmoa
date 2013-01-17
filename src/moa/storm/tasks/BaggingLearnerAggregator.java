package moa.storm.tasks;

import java.util.Random;

import moa.classifiers.Classifier;
import moa.core.MiscUtils;
import moa.evaluation.ClassificationPerformanceEvaluator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

public class BaggingLearnerAggregator implements ReducerAggregator<LearnerWrapper> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Random classifierRandom = new Random();
	private LearnerWrapper learner;
	
	/** 
	 * 
	 * @param c - wrapper for the classifier/evaluator pair
	 */
	public BaggingLearnerAggregator( LearnerWrapper c)
	{
		learner = c;
	}

	public LearnerWrapper init() {
		return learner;
	}

	public LearnerWrapper reduce(LearnerWrapper curr, TridentTuple tuple) {
		if (curr == null)
			curr = init();
		Object value = tuple.getValue(1);
		if (value instanceof Instance){
			int weight =  MiscUtils.poisson(1.0, this.classifierRandom);
			if (weight > 0) {
				Instance trainInst = (Instance) ((Instance) value).copy();
				trainInst.setWeight(trainInst.weight() * weight);
				curr.getClassifier().trainOnInstance(trainInst);
				curr.increaseInstancesProcessed();
				
			}
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