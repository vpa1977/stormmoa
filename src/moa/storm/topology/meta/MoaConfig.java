package moa.storm.topology.meta;

import backtype.storm.Config;

public class MoaConfig extends Config {

	private static final long serialVersionUID = 6137694002954230959L;

	@Override
	public void setNumWorkers(int workers) {
		m_num_workers = workers;
		super.setNumWorkers(workers);
	}

	public int getNumWorkers() {
		return m_num_workers;
	}

	public int getEnsembleSize() {
		return m_ensemble_size;
	}

	public void setEnsembleSize(int ensemble_size) {
		this.m_ensemble_size = ensemble_size;
	}

	public int getNumClassifierExecutors() {
		return m_num_classifier_executors;
	}

	public void setNumClassifierExecutors(int num_classifier_executors) {
		this.m_num_classifier_executors = num_classifier_executors;
	}

	public int getNumCombiners() {
		return m_num_combiners;
	}

	public void setNumCombiners(int num_combiners) {
		this.m_num_combiners = num_combiners;
	}

	public int getNumAggregators() {
		return m_num_aggregators;
	}

	public void setNumAggregators(int num_aggregators) {
		this.m_num_aggregators = num_aggregators;
	}

	private int m_num_workers;
	private int m_ensemble_size;
	private int m_num_classifier_executors;
	private int m_num_combiners;
	private int m_num_aggregators;

}
