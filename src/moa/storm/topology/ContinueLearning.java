package moa.storm.topology;

import java.io.Serializable;

public class ContinueLearning extends EnsembleCommand  implements Serializable{
	public ContinueLearning(long version) {
		super(version);
	}
}