package moa.storm.topology.meta;

import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.ensemble_members.BaggingMember;
import moa.storm.topology.grouping.AllGrouping;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.grouping.ShuffleLocalGrouping;
import moa.storm.topology.meta.bolts.CombinerBolt;
import moa.storm.topology.meta.bolts.EvaluateSpout;
import moa.storm.topology.meta.bolts.TopologyBroadcastBolt;
import moa.storm.topology.meta.bolts.WorkerBroadcastBolt;
import moa.storm.topology.meta.bolts.partitioned.PartitionedCombinerBolt;
import moa.storm.topology.meta.bolts.partitioned.PartitionedSharedStorageBolt;
import moa.storm.topology.meta.bolts.partitioned.SaveBolt;
import moa.storm.topology.meta.bolts.partitioned.VersionUpdateBolt;
import moa.storm.topology.meta.bolts.partitioned.ozabag.BagEvaluateBolt;
import moa.storm.topology.meta.bolts.partitioned.ozabag.BaggingLearningSpout;
import moa.storm.topology.meta.bolts.partitioned.ozabag.BaggingTopologyBroadcastBolt;
import moa.storm.topology.meta.bolts.partitioned.ozabag.BaggingTrainBolt;
import moa.storm.topology.spout.InstanceStreamSource;
import moa.storm.topology.spout.LearnSpout;
import performance.AllLocalGrouping;
import performance.LocalGrouping;
import backtype.storm.topology.TopologyBuilder;

public class PartitionedOzaBag {

	public PartitionedOzaBag() {
		super();
	}

	public void buildLearnPart(IStateFactory cassandra,
			InstanceStreamSource source, TopologyBuilder builder,
			String clasisifer_cli, MoaConfig config) {

		int num_workers = config.getNumWorkers();
		int ensemble_size = config.getEnsembleSize();
		int num_classifier_executors = config.getNumClassifierExecutors();

		BaggingLearningSpout learn_spout = new BaggingLearningSpout(source,
				cassandra, 100);
		builder.setSpout("learner_stream", learn_spout);
		BaggingTopologyBroadcastBolt broadcast = new BaggingTopologyBroadcastBolt(
				"learn", BaggingLearningSpout.LEARN_STREAM_FIELDS);

		builder.setBolt("deserialize", broadcast, num_workers).shuffleGrouping(
				"learner_stream", LearnSpout.EVENT_STREAM);

		builder.setBolt(
				"learn_local_grouping",
				new WorkerBroadcastBolt("learn",
						BaggingLearningSpout.LEARN_STREAM_FIELDS), num_workers)
				.customGrouping("deserialize", "learn", new AllGrouping());

		builder.setBolt(
				"train_classifier",
				new BaggingTrainBolt(ensemble_size, broadcast.getFields(),
						clasisifer_cli, cassandra),
				Math.max(num_classifier_executors, num_workers))
				.setNumTasks(Math.max(num_classifier_executors, num_workers))
				.customGrouping("learn_local_grouping", "learn",
						new AllLocalGrouping())
				.customGrouping("learner_stream",
						LearnSpout.NOTIFICATION_STREAM, new AllGrouping());

		builder.setBolt("persist", new SaveBolt<BaggingMember>(cassandra),
				Math.max(num_classifier_executors, num_workers))
				.setNumTasks(Math.max(num_classifier_executors, num_workers))
				.customGrouping("train_classifier", "persist",
						new ShuffleLocalGrouping());
		builder.setBolt("version_update",
				new VersionUpdateBolt(cassandra, "persist")).setNumTasks(1)
				.shuffleGrouping("persist", "persist_notify");

	}

	public void buildEvaluatePart(IStateFactory cassandra,
			InstanceStreamSource source, TopologyBuilder builder,
			MoaConfig config) {
		int num_workers = config.getNumWorkers();
		int ensemble_size = config.getEnsembleSize();
		int num_classifier_executors = config.getNumClassifierExecutors();
		int num_combiners = config.getNumCombiners();
		int num_aggregators = config.getNumAggregators();

		EvaluateSpout evaluate_spout = new EvaluateSpout(source, cassandra, 100);
		builder.setSpout("prediction_stream", evaluate_spout);

		builder.setBolt(
				"shared_storage",
				new PartitionedSharedStorageBolt(ensemble_size, cassandra,
						"evaluate_classifier"), num_workers).customGrouping(
				"prediction_stream", EvaluateSpout.NOTIFICATION_STREAM,
				new AllGrouping());

		builder.setBolt(
				"p_deserialize",
				new TopologyBroadcastBolt("evaluate",
						EvaluateSpout.EVALUATE_STREAM_FIELDS), num_workers)
				.shuffleGrouping("prediction_stream");

		builder.setBolt(
				"evaluate_local_grouping",
				new WorkerBroadcastBolt("evaluate",
						EvaluateSpout.EVALUATE_STREAM_FIELDS), num_workers)
				.customGrouping("p_deserialize", "evaluate", new AllGrouping());

		builder.setBolt("evaluate_classifier",
				new BagEvaluateBolt(ensemble_size, cassandra),
				Math.max(num_classifier_executors, num_workers))
				.customGrouping("evaluate_local_grouping", "evaluate",
						new AllLocalGrouping())
				.setNumTasks(Math.max(num_classifier_executors, num_workers));

		builder.setBolt(
				"combine_result",
				new PartitionedCombinerBolt(ensemble_size,
						"evaluate_classifier"),
				Math.max(num_workers, num_combiners))
				.customGrouping("evaluate_classifier",
						new LocalGrouping(new IdBasedGrouping()))
				.setNumTasks(Math.max(num_workers, num_combiners));

		builder.setBolt("prediction_result", new CombinerBolt(ensemble_size),
				Math.max(num_workers, num_combiners))
				.customGrouping("combine_result", new IdBasedGrouping())
				.setNumTasks(Math.max(num_workers, num_aggregators));

	}

}