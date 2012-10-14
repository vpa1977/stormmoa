package moa.storm.topology;

import java.util.ArrayList;
import java.util.Map;

import moa.core.DoubleVector;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class BaggingAggregator implements Aggregator<DoubleVector> {
	
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public DoubleVector init(Object batchId, TridentCollector collector) {
		return new DoubleVector();
	}

	@Override
	public void aggregate(DoubleVector val, TridentTuple tuple,
			TridentCollector collector) {
		DoubleVector vote = new DoubleVector( (double[]) tuple.getValueByField(LearnEvaluateTopology.FIELD_PREDICTION));
		if (vote.sumOfValues() > 0.0) {
            vote.normalize();
            val.addValues(vote);
        }
	}

	@Override
	public void complete(DoubleVector val, TridentCollector collector) {
		ArrayList<Object> output = new ArrayList<Object>();
		output.add(val);
		collector.emit(output);

	}

}
