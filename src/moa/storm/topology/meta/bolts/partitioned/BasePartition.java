package moa.storm.topology.meta.bolts.partitioned;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

public abstract class BasePartition extends BaseRichBolt {


	protected int m_ensemble_size;
	protected int m_start_key;
	protected int m_partition_size;
	protected boolean m_blast;

	public BasePartition(int ensemble_size) {
		super();
		m_ensemble_size = ensemble_size;
	}

	protected void preparePartition(TopologyContext context) {
		List<Integer> me = context.getComponentTasks(context.getThisComponentId());
		int tasks_size = me.size();
		m_blast = context.getThisTaskIndex() == me.size()-1;  
		int size = m_ensemble_size /tasks_size ;
		m_start_key =  size * context.getThisTaskIndex();
		// adjust size of the last partition
		if (m_blast )
		{
			int allocated = (tasks_size - 1)* size;
			size = m_ensemble_size - allocated;
		}
		m_partition_size = size;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		preparePartition(context);
		
	}

}