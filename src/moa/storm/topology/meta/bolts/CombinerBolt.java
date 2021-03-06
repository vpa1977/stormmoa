package moa.storm.topology.meta.bolts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import moa.core.DoubleVector;
import performance.Prediction;
import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CombinerBolt extends BaseRichBolt implements IRichBolt 
{
	private long m_combiner_stat = 0;
	private long m_tuple_stat = 0;
	private int m_ensemble_size;
	private OutputCollector m_collector;
	private LoadingCache<Object,Prediction> m_predictions;
	private int m_task_id;
	private boolean m_combiner = false;
	private long m_emit_time;
	private String m_component_name;

	public CombinerBolt(String component_name)
	{
		m_component_name = component_name;
		m_ensemble_size = -1;
	}

	public CombinerBolt(int ensemble_size)
	{
		m_ensemble_size = ensemble_size;

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		int message_timeout = 30;
		try {
			message_timeout = Integer.parseInt(String.valueOf(stormConf.get("topology.message.timeout.secs")));
		} catch (NumberFormatException e) {}
		m_predictions = CacheBuilder.newBuilder().expireAfterWrite(message_timeout, TimeUnit.SECONDS).build(new CacheLoader<Object,Prediction>(){

			@Override
			public Prediction load(Object key) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			
		});
		
				//new HashMap<Object, Prediction>();
		m_task_id = context.getThisTaskId();
		if (m_component_name != null)
		{
			m_combiner = true;
			m_ensemble_size = 0;
			List<Integer> alltasks = context.getThisWorkerTasks();
			List<Integer> classifiers = context.getComponentTasks(m_component_name);
			for (Integer n : classifiers)
			{
				if ( alltasks.contains(n))
				{
					m_ensemble_size++;	
				}
			}
			System.out.println("Combiner ensemble size "+ m_ensemble_size);
			if (m_ensemble_size < 1 )
				throw new RuntimeException("Unable to instantiate combiner - ensemble size is less than 1");
		}
	}

	@Override
	public void execute(Tuple input) {
		int numVotes = input.getIntegerByField("votes").intValue();
		m_emit_time += System.currentTimeMillis()  - input.getLong(4).longValue();

		Object instance_id = input.getValue(0);
		ArrayList<Instance> instance = (ArrayList<Instance>)input.getValue(1);
		ArrayList<DoubleVector> vect = (ArrayList<DoubleVector>)input.getValue(2) ;


		Prediction prediction = m_predictions.getIfPresent(instance_id);
		if (prediction == null) {
			prediction = new Prediction(instance, vect, input, numVotes);
			m_predictions.put( instance_id, prediction);
		}
		else {
			prediction.addVotes( vect , numVotes);
			m_collector.ack(input);
		}
		m_tuple_stat ++;
		if (m_tuple_stat % 10000 == 0 && !m_combiner)
		{
			long size = m_predictions.size();
			int half_count =0;
			Iterator<Entry<Object,Prediction>> it = m_predictions.asMap().entrySet().iterator();
			while  (it.hasNext())
			{
				if (it.next().getValue().m_num_votes == 2) 
				{
					half_count ++;
				}
			}
			System.out.println("Tuple Stat "+ m_tuple_stat + " for " + m_task_id + " avg emit "+ (m_emit_time/m_tuple_stat));
			System.out.println("Half_count "+ half_count + " out of "+ size + " for tuples "+ (m_tuple_stat/m_ensemble_size));
		}

		if (prediction.m_num_votes == m_ensemble_size)
		{
			ArrayList<Object> output = new ArrayList<Object>();
			output.add(instance_id);
			output.add(new ArrayList<Instance>());

			for (DoubleVector d : prediction.m_votes)
			{
				if (d.sumOfValues() > 0)
					d.normalize();
			}

			output.add(prediction.m_votes);
			output.add(prediction.m_num_votes);
			output.add(System.currentTimeMillis());
			m_collector.emit(prediction.m_input,output);
			m_collector.ack(prediction.m_input);
			
			m_predictions.invalidate(instance_id);
			m_predictions.cleanUp();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance_id", "instance", "prediction","votes", "timestamp"));
	}

}