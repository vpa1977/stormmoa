package moa.storm.topology.message;

import java.io.Serializable;


public class Reset extends EnsembleCommand  implements Serializable{
	private long m_pending;
	public Reset(long version, long pending) {
		super(version);
		m_pending = pending;
	}

	public long pending() {
		return m_pending;
	}
}