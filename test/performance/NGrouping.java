package performance;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import moa.storm.topology.message.MessageIdentifier;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class NGrouping implements CustomStreamGrouping {
	
	private int m_n;
	
	ArrayList< ArrayList<Integer> > m_tasks;

	public NGrouping(int n)
	{
		m_n = n;
		 
	}
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		m_tasks = new ArrayList< ArrayList<Integer> >();
		for (int i = 0 ;i < m_n ; i ++ )
		{
			m_tasks.add ( new ArrayList<Integer>());
		}
		Iterator<Integer> it = targetTasks.iterator();
		while (it.hasNext())
		{
			for (int i = 0 ;i  <m_n ; i++)
			{
				if (it.hasNext())
					m_tasks.get(i).add(it.next());
				else
					break;
			}
		}

	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		MessageIdentifier identifier = (MessageIdentifier)values.get(0);
		int set_index = (int)(identifier.getId() % m_n);
		return  m_tasks.get(set_index);
	}

}
