package moa.storm.persistence.ensemble_members;

import java.io.Serializable;


public class BaggingMember extends EnsembleMember implements Serializable
{	
	public BaggingMember(){}
	
	
	public BaggingMember(EnsembleMember m) {
		super(m);
	}
}