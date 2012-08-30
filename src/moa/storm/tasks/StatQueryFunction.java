package moa.storm.tasks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import moa.classifiers.Classifier;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.QueryFunction;
import storm.trident.state.State;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;

public class StatQueryFunction extends BaseQueryFunction<ReadOnlySnapshottable<LearnerWrapper>, LearnerWrapper> implements Serializable{

	public List<LearnerWrapper> batchRetrieve(ReadOnlySnapshottable<LearnerWrapper> state,
			List<TridentTuple> args) {
		LearnerWrapper theClassifier = state.get();
		
		ArrayList<LearnerWrapper> list = new ArrayList<LearnerWrapper>();
		for (TridentTuple t : args)
		{
			list.add( theClassifier);
		}
		return list;
	}
	
	@Override
	public void execute(TridentTuple tuple, LearnerWrapper result,
			TridentCollector collector) {
		ArrayList<Object> out = new ArrayList<Object>();
		out.add( new Long(result.getInstancesProcessed()));
		collector.emit(out);
		
	}


}
