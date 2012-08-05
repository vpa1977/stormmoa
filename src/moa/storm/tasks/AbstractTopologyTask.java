package moa.storm.tasks;

import backtype.storm.topology.TopologyBuilder;
import moa.core.ObjectRepository;
import moa.evaluation.LearningEvaluation;
import moa.tasks.AbstractTask;
import moa.tasks.TaskMonitor;

public abstract class AbstractTopologyTask extends AbstractTask {
	
	private TopologyBuilder m_topology_builder;
	
	protected TopologyBuilder getTopologyBuilder()
	{
		if (m_topology_builder == null)
			m_topology_builder = new TopologyBuilder();
		return m_topology_builder;
	}
	
	

	
	public Class<?> getTaskResultType() {
		return LearningEvaluation.class;
	}
	

	@Override
	protected abstract Object doTaskImpl(TaskMonitor monitor, ObjectRepository repository);

}
