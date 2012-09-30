package moa.storm.tasks;

import moa.classifiers.Classifier;
import moa.evaluation.ClassificationPerformanceEvaluator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;

public class EvaluationAggregator implements ReducerAggregator<LearnerWrapper> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private LearnerWrapper learner;
	
	/** 
	 * 
	 * @param c - wrapper for the classifier/evaluator pair
	 * @param learn - reducer mode - learn/evaluate
	 */
	public EvaluationAggregator( LearnerWrapper c)
	{
		learner = c;
	}

	public LearnerWrapper init() {
		return learner;
	}

	public LearnerWrapper reduce(LearnerWrapper curr, TridentTuple tuple) {
		if (curr == null)
			curr = init();
		Object prediction = tuple.getValue(0);
		Object value = tuple.getValue(1);
		if (value instanceof Instance){
			Instance inst = (Instance) value;
			curr.addEvaluation( prediction ,inst);
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
		return curr;
	}
}