package moa.storm.persistence.ensemble_members;

import java.io.Serializable;

public class BoostingMember extends EnsembleMember implements Serializable
{	

	public BoostingMember(){}
	
	
	public BoostingMember(BoostingMember m) {
		m_classifier = m.m_classifier.copy();
		m_scms = m.m_scms;
		m_swms = m.m_swms;
		m_training_weight_seen_by_model = m.m_training_weight_seen_by_model;
		m_key = m.m_key;
	}

	@Override
	public EnsembleMember copy() {
		return new BoostingMember(this);
	}

	
	
	public double m_scms;
	public double m_swms;
	public double m_training_weight_seen_by_model;
}