package performance.state;

import moa.storm.persistence.IPersistentState;

public class DummyPersistentState implements IPersistentState
{
	public static long the_long;
	public static Object the_object;

	@Override
	public long getLong(String row, String column) {
		// TODO Auto-generated method stub
		return the_long;
	}

	@Override
	public void setLong(String row, String column, long value) {
		the_long = value;
		
	}

	@Override
	public Object get(String row, String column) {
		// TODO Auto-generated method stub
		return the_object;
	}

	@Override
	public void put(String rowKey, String key, Object value) {
		the_object = value;
		
	}

	@Override
	public void deleteRow(String rowKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteColumn(String rowKey, String columnKey) {
		// TODO Auto-generated method stub
		
	}
	
}