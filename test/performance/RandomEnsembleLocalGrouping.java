package performance;

import java.util.ArrayList;
import java.util.List;

import moa.storm.topology.MessageIdentifier;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class RandomEnsembleLocalGrouping implements CustomStreamGrouping {
	
	
	private ArrayList< ArrayList<Integer> > m_tasks;
	private int m_ensemble_size;
	private int m_n_ensemles;
	
	public RandomEnsembleLocalGrouping(int ensemble_size)
	{
		m_ensemble_size = ensemble_size;
	}
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		List<Integer> thisWorkerTasks = context.getThisWorkerTasks();
		m_tasks = new ArrayList< ArrayList<Integer> >();
		ArrayList<Integer> currentTasks = new ArrayList<Integer>();
		for (Integer i : targetTasks)
		{
			if ( thisWorkerTasks.contains(i))
			{
				currentTasks.add(i);
				if (currentTasks.size() == m_ensemble_size)
				{
					m_tasks.add(currentTasks);
					currentTasks = new ArrayList<Integer>();
				}
			}
		}
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		MessageIdentifier id = (MessageIdentifier)values.get(0);
		int index = id.hashCode() % m_tasks.size();
		return m_tasks.get(index);
	}

}
