package moa.storm.topology;

import java.io.Serializable;

public class MessageIdentifier implements Serializable
{
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return m_task_id + " "+ m_id;
	}
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MessageIdentifier)
		{
			MessageIdentifier id = (MessageIdentifier)obj;
			return (id.m_task_id == m_task_id) && (id.m_id == m_id);
		}
		return false;
	}
	@Override
	public int hashCode() {
		return (int)m_id;
	}
	public MessageIdentifier( int task_id, long id)
	{
		m_id = id;
		m_task_id = task_id;
			
	}
	
	int m_task_id;
	long m_id;
	
	public long getId() {
		return m_id;
	}
	public int getTask() {
		return m_task_id;
	}
}