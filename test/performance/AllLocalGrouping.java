package performance;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class AllLocalGrouping implements CustomStreamGrouping {
	


	private ArrayList<Integer> m_available_tasks;

	public AllLocalGrouping() {
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		m_available_tasks = new ArrayList<Integer>();
		List<Integer> thisWorkerTasks = context.getThisWorkerTasks();
		for (Integer i : targetTasks)
		{
			if ( thisWorkerTasks.contains(i))
				m_available_tasks.add(i);
		}
	}
  
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		if (m_available_tasks.size() > 0 ) 
		{
			return m_available_tasks;
		}
		return new ArrayList<Integer>();
	}

}
