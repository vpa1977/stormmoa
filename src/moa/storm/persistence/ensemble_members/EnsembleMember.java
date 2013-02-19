package moa.storm.persistence.ensemble_members;

import java.io.Serializable;

import moa.classifiers.Classifier;

public class EnsembleMember implements Serializable {
	
	public EnsembleMember() {}
	
	public EnsembleMember(EnsembleMember m)
	{
		m_classifier = m.m_classifier.copy();
		m_key = m.m_key;
	}
	
	public EnsembleMember copy()
	{
		EnsembleMember newMember = new EnsembleMember(this);
		return newMember;
	}

	public Classifier m_classifier;
	public String m_key;
	



}
