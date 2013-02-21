package moa.storm.topology.meta;

import java.io.Serializable;

import moa.classifiers.Classifier;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.SharedStorageBolt;
import moa.storm.topology.grouping.AllGrouping;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.meta.bolts.CombinerBolt;
import moa.storm.topology.meta.bolts.EvaluateClassifierBolt;
import moa.storm.topology.meta.bolts.EvaluateSpout;
import moa.storm.topology.meta.bolts.TopologyBroadcastBolt;
import moa.storm.topology.meta.bolts.TrainClassifierBolt;
import moa.storm.topology.meta.bolts.WorkerBroadcastBolt;
import moa.storm.topology.spout.InstanceStreamSource;
import moa.storm.topology.spout.LearnSpout;
import performance.AllLocalGrouping;
import performance.LocalGrouping;
import backtype.storm.topology.TopologyBuilder;

public class OzaBag implements Serializable {

	public OzaBag() {
	}

	public  void buildLearnPart(IStateFactory cassandra, InstanceStreamSource source, TopologyBuilder builder,
			String clasisifer_cli, int num_workers, int ensemble_size, int num_classifier_executors) {
				LearnSpout learn_spout = new LearnSpout(source, cassandra, 100);
				builder.setSpout("learner_stream",  learn_spout);
				builder.setBolt("deserialize", new TopologyBroadcastBolt("learn", LearnSpout.LEARN_STREAM_FIELDS),num_workers).shuffleGrouping("learner_stream",LearnSpout.EVENT_STREAM);
				
				builder.setBolt("learn_local_grouping", new WorkerBroadcastBolt("learn", LearnSpout.LEARN_STREAM_FIELDS), num_workers)
					.customGrouping("deserialize", "learn", new AllGrouping());
					
				
				builder.setBolt("train_classifier", new TrainClassifierBolt(clasisifer_cli,cassandra ),Math.max(num_classifier_executors,num_workers))
					.setNumTasks(ensemble_size)
					.customGrouping("learn_local_grouping", "learn", new AllLocalGrouping())
					.customGrouping("learner_stream", LearnSpout.NOTIFICATION_STREAM, new AllGrouping());
			}

	public void buildEvaluatePart(IStateFactory cassandra, InstanceStreamSource source, TopologyBuilder builder,
			int num_workers, int ensemble_size, int num_classifier_executors, int num_combiners, int num_aggregators) {
				EvaluateSpout evaluate_spout = new EvaluateSpout(source, cassandra, 100);
				builder.setSpout("prediction_stream", evaluate_spout);
				
				
				builder.setBolt("shared_storage", new SharedStorageBolt<Classifier>(cassandra, "evaluate_classifier"), num_workers).customGrouping("prediction_stream", EvaluateSpout.NOTIFICATION_STREAM,new AllGrouping());
				
				builder.setBolt("p_deserialize", new TopologyBroadcastBolt("evaluate", LearnSpout.LEARN_STREAM_FIELDS),num_workers).shuffleGrouping("prediction_stream");
				
				builder.setBolt("evaluate_local_grouping", new WorkerBroadcastBolt("evaluate", LearnSpout.LEARN_STREAM_FIELDS), num_workers).customGrouping("p_deserialize", "evaluate", new AllGrouping());
			
				builder.setBolt("evaluate_classifier", new EvaluateClassifierBolt(cassandra),Math.max(num_classifier_executors,num_workers))
					.customGrouping("evaluate_local_grouping", "evaluate", new AllLocalGrouping())
					.setNumTasks(ensemble_size);
				
				builder.setBolt("combine_result", new CombinerBolt ("evaluate_classifier"), Math.max(num_workers, num_combiners))
					.customGrouping("evaluate_classifier", new LocalGrouping( new IdBasedGrouping()))
					.setNumTasks(Math.max(num_workers, num_combiners));
			
				builder.setBolt("aggregate_result", new CombinerBolt(ensemble_size), Math.max(num_workers, num_combiners))
					.customGrouping("combine_result", new IdBasedGrouping())
					.setNumTasks(Math.max(num_workers, num_aggregators) );
			
				
			}

}