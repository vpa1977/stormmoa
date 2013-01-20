package performance;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class CombinerTest implements CombinerAggregator<Object> {

	@Override
	public Object init(TridentTuple tuple) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object combine(Object val1, Object val2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object zero() {
		// TODO Auto-generated method stub
		return null;
	}

}
