package moa.storm.topology;

import java.util.ArrayList;
import java.util.List;


import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class AllGrouping implements CustomStreamGrouping {
	
	private List<Integer> m_available_tasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		m_available_tasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		return m_available_tasks;
	}

}
