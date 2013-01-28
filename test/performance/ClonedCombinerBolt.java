package performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import moa.core.DoubleVector;
import moa.storm.topology.ClassifierBolt;
import weka.core.Instance;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ClonedCombinerBolt extends BaseRichBolt implements IRichBolt 
{
	private long m_combiner_stat = 0;
	private long m_tuple_stat = 0;
	private int m_ensemble_size;
	private OutputCollector m_collector;
	private HashMap<Object,Prediction> m_predictions;
	private int m_task_id;
	private boolean m_combiner = false;
	private long m_emit_time;
	public ClonedCombinerBolt(int ensemble_size)
	{
		m_ensemble_size = ensemble_size;

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		m_collector = collector;
		m_predictions = new HashMap<Object, Prediction>();
		m_task_id = context.getThisTaskId();
		int assigned_tasks = 0;
		List<Integer> alltasks = context.getThisWorkerTasks();
		List<Integer> classifiers = context.getComponentTasks("evaluator");
		for (Integer n : classifiers)
		{
			if ( alltasks.contains(n))
			{
				assigned_tasks++;	
			}
		}
		if (assigned_tasks % m_ensemble_size > 0) 
			throw new RuntimeException("Unable to deploy the topology - the ensemble is split between machines");
		
	}

	@Override
	public void execute(Tuple input) {
		if (m_ensemble_size == 0)
		{
			m_ensemble_size = ClassifierBolt.INSTANCES;
			m_combiner = true;
		}
		int numVotes = input.getInteger(3).intValue();
		
		Object instance_id = input.getValue(0);
		ArrayList<Instance> instance = (ArrayList<Instance>)input.getValue(1);
		ArrayList<DoubleVector> vect = (ArrayList<DoubleVector>)input.getValue(2) ;
		
		
		
		
		Prediction prediction = m_predictions.get(instance_id);
		if (prediction == null) {
			prediction = new Prediction(instance, vect, input, numVotes);
			m_predictions.put( instance_id, prediction);
		}
		else {
			prediction.addVotes( vect , numVotes);
			m_collector.ack(input);
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
			output.add( prediction.m_num_votes);
			m_collector.emit(prediction.m_input,output);
			m_collector.ack(prediction.m_input);
			m_predictions.remove(instance_id);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("instance_id", "instance", "prediction","votes"));
	}
	
}