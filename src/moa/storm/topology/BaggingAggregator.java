package moa.storm.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.log4j.Logger;


import java.util.Map;

import moa.core.DoubleVector;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;


class Prediction
{
	public Prediction(long id, Instance inst, DoubleVector vote)
	{
		m_id = id;
		m_instance = inst;
		m_data = vote;
	}
            	

	public Instance m_instance;
	public DoubleVector m_data;
	public long m_id;
}

public class BaggingAggregator implements Aggregator< HashMap<Long, Prediction> > {
	
	public static Logger LOG = Logger.getLogger(BaggingAggregator.class);
	private Object batch;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		LOG.info("Instantiated Bagging Aggregator");
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	/** 
	 * Collect data 
	 */
	@Override
	public  HashMap<Long, Prediction> init(Object batchId, TridentCollector collector) {
		batch = batchId;
		return new  HashMap<Long, Prediction>();
	}

	@Override
	public void aggregate(HashMap<Long, Prediction> val, TridentTuple tuple,
			TridentCollector collector) {
		Long id = (Long)tuple.getValueByField(LearnEvaluateTopology.FIELD_ID);
		DoubleVector vote = new DoubleVector( (double[]) tuple.getValueByField(LearnEvaluateTopology.FIELD_PREDICTION));
		Instance inst = (Instance)tuple.getValueByField(LearnEvaluateTopology.FIELD_INSTANCE);
		if (vote.sumOfValues() > 0.0) {
            vote.normalize();
            Prediction prediction = val.get(id);
            if (prediction == null)
            {
            	prediction = new Prediction(id,inst,vote);
            	val.put(id, prediction);
            }
            else
            {
            	prediction.m_data.addValues(vote);
            	
            }
            
        }
	}

	@Override
	public void complete( HashMap<Long, Prediction> val, TridentCollector collector) {
		LOG.info("Completed BATCH>"+batch);
		Iterator<Prediction> predictions = val.values().iterator();
		while (predictions.hasNext())
		{
			Prediction p = predictions.next();
			p.m_data.normalize();
			ArrayList<Object> output = new ArrayList<Object>();
			output.add(p.m_instance);
			output.add(p.m_data);
			collector.emit(output);			
		}
	}

}
