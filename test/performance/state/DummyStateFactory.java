package performance.state;

import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;

public class DummyStateFactory implements IStateFactory 
{
	public IPersistentState the_state;

	@Override
	public IPersistentState create() {
		// TODO Auto-generated method stub
		return the_state;
	}
	
}