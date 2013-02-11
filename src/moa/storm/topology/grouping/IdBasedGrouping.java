package moa.storm.topology.grouping;

import java.util.ArrayList;
import java.util.List;

import moa.storm.topology.message.MessageIdentifier;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class IdBasedGrouping implements CustomStreamGrouping {
	
	private List<Integer> m_available_tasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		m_available_tasks = targetTasks;
		
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		MessageIdentifier identifier = (MessageIdentifier)values.get(0);
		int task_index = (int)(identifier.getId() % m_available_tasks.size());
		ArrayList<Integer> i = new ArrayList<Integer>();
		i.add(m_available_tasks.get(task_index));
		return i;
	}

}
