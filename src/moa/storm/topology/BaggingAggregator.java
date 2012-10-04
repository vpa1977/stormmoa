package moa.storm.topology;

import java.util.Map;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class BaggingAggregator implements Aggregator {
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Object init(Object batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void aggregate(Object val, TridentTuple tuple,
			TridentCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void complete(Object val, TridentCollector collector) {
		// TODO Auto-generated method stub

	}

}
