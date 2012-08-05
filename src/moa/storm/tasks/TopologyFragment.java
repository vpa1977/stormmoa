package moa.storm.tasks;

import backtype.storm.topology.TopologyBuilder;
import moa.core.ObjectRepository;
import moa.options.AbstractOptionHandler;
import moa.tasks.TaskMonitor;

/** 
 * A topology fragment used to assemble the final MOA topology 
 */
public abstract class TopologyFragment extends AbstractOptionHandler {
	
	
	public abstract TopologyBuilder build( TopologyBuilder b);

	
	public void getDescription(StringBuilder sb, int indent) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void prepareForUseImpl(TaskMonitor monitor,
			ObjectRepository repository) {
		// TODO Auto-generated method stub
		
	}

}
