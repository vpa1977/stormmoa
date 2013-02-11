package moa.storm.topology.meta;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import moa.storm.topology.grouping.AllGrouping;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.meta.bolts.ClassifierBolt;
import moa.storm.topology.meta.bolts.TopologyBroadcastBolt;
import moa.storm.topology.meta.bolts.WorkerBroadcastBolt;
import performance.AllLocalGrouping;
import performance.CombinerBolt;
import performance.LocalGrouping;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;

public class MemoryOnlyOzaBag {

	public static final List<String> FIELDS = Arrays.asList(new String[]{"instance_id", "instance"});
	

	public void build(String classifier, TopologyBuilder builder, int ensemble_size, int num_workers,
			int num_classifiers, int num_combiners, int num_aggregators, IRichSpout learn, IRichSpout predict) {
				
				builder.setSpout("prediction_stream", predict);
				builder.setSpout("learner_stream", learn);
				
				
				builder.setBolt("p_deserialize", new TopologyBroadcastBolt("evaluate", FIELDS ),num_workers).shuffleGrouping("prediction_stream");
				builder.setBolt("deserialize", new TopologyBroadcastBolt("learn", FIELDS),num_workers).shuffleGrouping("learner_stream");
				
				builder.setBolt("evaluate_local_grouping", new WorkerBroadcastBolt("evaluate",FIELDS), num_workers).customGrouping("p_deserialize", "evaluate", new AllGrouping());
				
				builder.setBolt("learn_local_grouping", new WorkerBroadcastBolt("learn",FIELDS), num_workers).customGrouping("deserialize", "learn", new AllGrouping());
				
				builder.setBolt("classifier_instance", new ClassifierBolt(classifier),Math.max(num_classifiers,num_workers)).setNumTasks(ensemble_size).
					customGrouping("evaluate_local_grouping", "evaluate", new AllLocalGrouping()).
					customGrouping("learn_local_grouping", "learn", new AllLocalGrouping());
				
				
				builder.setBolt("combine_result", new CombinerBolt ("classifier_instance"), Math.max(num_workers, num_combiners)).customGrouping("classifier_instance", new LocalGrouping( new IdBasedGrouping()))
				.setNumTasks(Math.max(num_workers, num_combiners));
				
				builder.setBolt("prediction_result", new CombinerBolt(ensemble_size), Math.max(num_workers, num_combiners)).customGrouping("combine_result", new IdBasedGrouping()).setNumTasks(Math.max(num_workers, num_aggregators) );
				
				
			}

	public MemoryOnlyOzaBag() {
		super();

	}

}