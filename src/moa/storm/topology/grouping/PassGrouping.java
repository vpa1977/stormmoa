package moa.storm.topology.grouping;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class PassGrouping implements CustomStreamGrouping {

	private List<Integer> m_tasks;
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		m_tasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		ArrayList<Integer> ret = new ArrayList<Integer>();
		Number nIndex = (Number)values.get(1);
		int index =nIndex.intValue();
		if (index < m_tasks.size())
		{
			ret.add( m_tasks.get(index));
		}
		return ret;	
	}

}
