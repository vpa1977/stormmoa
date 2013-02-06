package performance.ozabag_distributed;

import java.io.Serializable;

import moa.classifiers.Classifier;

public class BaggingMember implements Serializable
{	
	public BaggingMember(){}
	
	
	public BaggingMember(BaggingMember m) {
		m_classifier = m.m_classifier.copy();
		m_key = m.m_key;
	}
	public Classifier m_classifier;
	public String m_key;
}