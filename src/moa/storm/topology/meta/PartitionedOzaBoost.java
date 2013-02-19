package moa.storm.topology.meta;

import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.ensemble_members.BoostingMember;
import moa.storm.topology.grouping.AllGrouping;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.grouping.PassGrouping;
import moa.storm.topology.grouping.ShuffleLocalGrouping;
import moa.storm.topology.meta.bolts.EvaluateSpout;
import moa.storm.topology.meta.bolts.TopologyBroadcastBolt;
import moa.storm.topology.meta.bolts.WorkerBroadcastBolt;
import moa.storm.topology.meta.bolts.partitioned.PartitionedCombinerBolt;
import moa.storm.topology.meta.bolts.partitioned.PartitionedSharedStorageBolt;
import moa.storm.topology.meta.bolts.partitioned.SaveBolt;
import moa.storm.topology.meta.bolts.partitioned.VersionUpdateBolt;
import moa.storm.topology.meta.bolts.partitioned.ozaboost.BoostEvaluateBolt;
import moa.storm.topology.meta.bolts.partitioned.ozaboost.BoostTopologyBroadcastBolt;
import moa.storm.topology.meta.bolts.partitioned.ozaboost.BoostTrainBolt;
import moa.storm.topology.meta.bolts.partitioned.ozaboost.BoostingLearningSpout;
import moa.storm.topology.spout.InstanceStreamSource;
import moa.storm.topology.spout.LearnSpout;
import performance.AllLocalGrouping;
import performance.CombinerBolt;
import performance.LocalGrouping;
import backtype.storm.topology.TopologyBuilder;

public class PartitionedOzaBoost {

	public PartitionedOzaBoost() {
		super();
	}

	public void buildLearnPart(IStateFactory cassandra, InstanceStreamSource source, TopologyBuilder builder,
			String clasisifer_cli, int num_workers, int ensemble_size, int num_classifier_executors) {
				BoostingLearningSpout learn_spout = new BoostingLearningSpout(source, cassandra, 100);
				builder.setSpout("learner_stream",  learn_spout);
				BoostTopologyBroadcastBolt broadcast = new BoostTopologyBroadcastBolt("learn", BoostingLearningSpout.LEARN_STREAM_FIELDS);
				
				builder.setBolt("deserialize",broadcast,num_workers).shuffleGrouping("learner_stream",LearnSpout.EVENT_STREAM);
				
				builder.setBolt("train_classifier", new BoostTrainBolt(ensemble_size,broadcast.getFields(),clasisifer_cli,cassandra ),Math.max(num_classifier_executors,num_workers))
					.setNumTasks(Math.max(num_classifier_executors,num_workers))
					.customGrouping("deserialize", "learn", new PassGrouping())
					.customGrouping("train_classifier", "learn", new PassGrouping())
					.customGrouping("learner_stream", LearnSpout.NOTIFICATION_STREAM, new AllGrouping());
				
				builder.setBolt("persist", new SaveBolt<BoostingMember>(cassandra), Math.max(num_classifier_executors,num_workers)).setNumTasks(Math.max(num_classifier_executors,num_workers))
					.customGrouping("train_classifier","persist", new ShuffleLocalGrouping());
				builder.setBolt("version_update", new VersionUpdateBolt(cassandra, "persist")).setNumTasks(1)
					.shuffleGrouping("persist", "persist_notify");
					
			}

	public void buildEvaluatePart(IStateFactory cassandra, InstanceStreamSource source, TopologyBuilder builder,
			int num_workers, int ensemble_size, int num_classifier_executors, int num_combiners, int num_aggregators) {
				EvaluateSpout evaluate_spout = new EvaluateSpout(source, cassandra, 100);
				builder.setSpout("prediction_stream", evaluate_spout);
				
				
				builder.setBolt("shared_storage", new PartitionedSharedStorageBolt(ensemble_size, cassandra, "evaluate_classifier"), num_workers)
					.customGrouping("prediction_stream", EvaluateSpout.NOTIFICATION_STREAM,new AllGrouping());
				
				builder.setBolt("p_deserialize", new TopologyBroadcastBolt("evaluate", EvaluateSpout.EVALUATE_STREAM_FIELDS),num_workers).shuffleGrouping("prediction_stream");
				
				builder.setBolt("evaluate_local_grouping", new WorkerBroadcastBolt("evaluate", EvaluateSpout.EVALUATE_STREAM_FIELDS), num_workers).customGrouping("p_deserialize", "evaluate", new AllGrouping());
			
				builder.setBolt("evaluate_classifier", new BoostEvaluateBolt(ensemble_size,cassandra),Math.max(num_classifier_executors,num_workers))
					.customGrouping("evaluate_local_grouping", "evaluate", new AllLocalGrouping())
					.setNumTasks(Math.max(num_classifier_executors,num_workers));
				
				builder.setBolt("combine_result", new PartitionedCombinerBolt (ensemble_size,"evaluate_classifier"), Math.max(num_workers, num_combiners))
					.customGrouping("evaluate_classifier", new LocalGrouping( new IdBasedGrouping()))
					.setNumTasks(Math.max(num_workers, num_combiners));
			
				builder.setBolt("aggregate_result", new CombinerBolt(ensemble_size), Math.max(num_workers, num_combiners))
					.customGrouping("combine_result", new IdBasedGrouping())
					.setNumTasks(Math.max(num_workers, num_aggregators) );
			
				
			}

}