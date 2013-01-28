package moa.storm.topology;

import java.io.Serializable;

public class EnsembleCommand  implements Serializable
{
	public EnsembleCommand(long version) {m_version = version;}
	
	public long version() { return m_version; }
	
	private long m_version;
}