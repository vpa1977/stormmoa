package moa.storm.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class EchoFunction extends BaseFunction {
	
	private int m_id;
	
	public EchoFunction(int id)
	{
		m_id = id;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(Map arg0, TridentOperationContext arg1) {
		
		
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if (tuple != null && tuple.getValues() != null && collector != null)
		{
			List<Object> values = tuple.getValues();
			ArrayList<Object> emit = new ArrayList<Object>();
			emit.add(m_id);
			emit.addAll(values);
			collector.emit(emit);
		}
	}
}
