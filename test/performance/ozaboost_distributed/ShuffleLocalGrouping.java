package performance.ozaboost_distributed;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ShuffleLocalGrouping implements CustomStreamGrouping {
	


	private ArrayList<Integer> m_available_tasks;
	private int m_task_index;

	public ShuffleLocalGrouping() {
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
		m_task_index =0;
	}
  
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		
		ArrayList<Integer> ret = new ArrayList<Integer>();
		if (m_available_tasks.size() > 0 ) 
		{
			int task = m_available_tasks.get(m_task_index++);
			if (m_task_index == m_available_tasks.size())
				m_task_index =0;
			ret.add(task);
			return ret;
		}
		return ret;
	}

}
