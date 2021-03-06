package moa.trident.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import moa.core.DoubleVector;

import org.apache.log4j.Logger;

import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;


class Prediction implements Serializable
{
	public Prediction(long id, Instance inst, DoubleVector vote)
	{
		m_id = id;
		m_instance = inst;
		m_data = vote;
	}
            	
	public long m_votes;
	public Instance m_instance;
	public DoubleVector m_data;
	public long m_id;
}

class BaggingCombiner implements CombinerAggregator< Prediction >
{

	@Override
	public Prediction init(TridentTuple tuple) {
		DoubleVector vote = new DoubleVector( (double[]) tuple.getValueByField(LearnEvaluateTopology.FIELD_PREDICTION));
		if (vote.sumOfValues() > 0.0) {
			vote.normalize();
		}
		Instance inst = (Instance)tuple.getValueByField(LearnEvaluateTopology.FIELD_INSTANCE);
		return new Prediction(0, inst, vote);
	}

	@Override
	public Prediction combine(Prediction val1, Prediction val2) {
		val2.m_data.addValues(val1.m_data);
		return val2;
	}

	@Override
	public Prediction zero() {
		// TODO Auto-generated method stub
		return new Prediction( 0, null, new DoubleVector());
	}
	
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
		Long id = (Long)tuple.getValueByField(LearnEvaluateTopology.FIELD_TUPLE_ID);
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
