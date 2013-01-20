package performance;

import java.util.ArrayList;
import java.util.List;

import moa.storm.topology.IdBasedGrouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class LocalGrouping implements CustomStreamGrouping {
	
	private IdBasedGrouping m_fallback;
	private IdBasedGrouping m_local;
	private ArrayList<Integer> m_available_tasks;

	public LocalGrouping(IdBasedGrouping idBasedGrouping) {
		m_fallback = idBasedGrouping;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		m_available_tasks = new ArrayList<Integer>();
		m_fallback.prepare(context, stream, targetTasks);
		List<Integer> thisWorkerTasks = context.getThisWorkerTasks();
		for (Integer i : targetTasks)
		{
			if ( thisWorkerTasks.contains(i))
				m_available_tasks.add(i);
		}
		m_local = new IdBasedGrouping();
		m_local.prepare(context, stream, m_available_tasks);

	}
  
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		if (m_available_tasks.size() > 0 ) 
		{
			return m_local.chooseTasks(taskId, values);
		}
		return m_fallback.chooseTasks(taskId, values);
	}

}
