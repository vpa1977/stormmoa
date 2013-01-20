package performance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import moa.core.DoubleVector;
import weka.core.Instance;
import backtype.storm.tuple.Tuple;

class Prediction implements Serializable
{
	public Prediction(ArrayList<Instance> instance, ArrayList<DoubleVector> vect, Tuple input, int numVotes) {
		m_instance = instance;
		m_votes = vect;
		m_input = input;
		m_num_votes = numVotes;
		for (DoubleVector d : m_votes)
		{
			if (d.sumOfValues() > 0)
				d.normalize();

		}
	}
	ArrayList<Instance> m_instance;
	ArrayList<DoubleVector> m_votes;
	int m_num_votes;
	long m_timestamp;
	Tuple m_input;
	
	public void addVotes(ArrayList<DoubleVector> vect, int numVotes) {
		Iterator<DoubleVector> my_votes = m_votes.iterator();
		Iterator<DoubleVector> inc_votes = vect.iterator();
		while (my_votes.hasNext()){
			DoubleVector d = inc_votes.next();
			if (d.sumOfValues() > 0)
				d.normalize();
			my_votes.next().addValues(d);
		}
		m_num_votes += numVotes;
	}
}