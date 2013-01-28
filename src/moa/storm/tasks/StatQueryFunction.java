package moa.storm.tasks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;

public class StatQueryFunction extends BaseQueryFunction<ReadOnlySnapshottable<LearnerWrapper>, LearnerWrapper> implements Serializable{

	public List<LearnerWrapper> batchRetrieve(ReadOnlySnapshottable<LearnerWrapper> state,
			List<TridentTuple> args) {
		
		try {
			LearnerWrapper theClassifier = state.get();
			ArrayList<LearnerWrapper> list = new ArrayList<LearnerWrapper>();
			for (TridentTuple t : args)
			{
				list.add( theClassifier);
			}
			return list;
		}
		catch (Throwable t)
		{
			// class cast exception and other interesting things if the cache is empty 
			// probably bug in trident. 
			ArrayList<LearnerWrapper> list = new ArrayList<LearnerWrapper>();
			list.add( new LearnerWrapper(null));
			return list;
		}
		
	}
	
	@Override
	public void execute(TridentTuple tuple, LearnerWrapper result,
			TridentCollector collector) {
		ArrayList<Object> out = new ArrayList<Object>();
		if (result != null)
			out.add( new Long(result.getInstancesProcessed()));
		else
			out.add( new Long(0));
		collector.emit(out);
	}


}
