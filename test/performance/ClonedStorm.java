package performance;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import moa.storm.persistence.CassandraState;
import moa.storm.persistence.HDFSState;
import moa.storm.persistence.IStateFactory;
import moa.storm.topology.grouping.IdBasedGrouping;
import moa.storm.topology.meta.OzaBag;
import moa.streams.generators.RandomTreeGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.Testing;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ClonedStorm extends OzaBag implements Serializable {

	public static final long GLOBAL_BATCH_SIZE = 100;

	static int INSTANCE = 0;
	public static Logger LOG = Logger.getLogger(ClonedStorm.class);

	class CounterBolt extends BaseRichBolt implements IRichBolt {

		private OutputCollector m_collector;

		long m_instance;
		long m_start = 0;
		long m_measurement_start = 0;
		long count = 0;
		long period = 0;

		final long MEASUREMENT_PERIOD = 1 * 60 * 1000;

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub

		}

		private int getPid() throws Throwable {
			java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory
					.getRuntimeMXBean();
			java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField(
					"jvm");
			jvm.setAccessible(true);
			sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm
					.get(runtime);
			java.lang.reflect.Method pid_method = mgmt.getClass()
					.getDeclaredMethod("getProcessId");
			pid_method.setAccessible(true);
			int pid = (Integer) pid_method.invoke(mgmt);
			return pid;
		}

		private void writeResult(long period) {
			try {

				long tup_sec = count * GLOBAL_BATCH_SIZE * 1000 / period;

				File f = new File("/home/vp37/trident_bench"
						+ InetAddress.getLocalHost().getHostName() + "-"
						+ getPid() + "-" + m_instance);
				FileOutputStream fos = new FileOutputStream(f);
				String result = "" + tup_sec;
				fos.write(result.getBytes());
				fos.write(" \r\n".getBytes());
				fos.flush();
				fos.close();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

		@Override
		public void execute(Tuple tuple) {
			m_collector.ack(tuple);
			if (m_start == 0)
				m_start = System.currentTimeMillis();
			long current = System.currentTimeMillis();
			count++;
			if (count % 10000 == 0)
				System.out.println("processed " + count);
			if (current - m_start > MEASUREMENT_PERIOD) {
				LOG.info("Writing Result");
				writeResult(current - m_start);
				m_start = System.currentTimeMillis();
				count = 0;
			}

		}

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			m_collector = collector;
			m_instance = INSTANCE++;
			System.out.println("New Instance " + INSTANCE);

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public ClonedStorm(Config conf, String[] args) throws Throwable {

		final int ensemble_size = Integer.parseInt(args[0]);
		final int num_workers = Integer.parseInt(args[1]);
		final int num_classifiers = Integer.parseInt(args[2]);
		final int num_combiners = Integer.parseInt(args[3]);
		final int num_aggregators = Integer.parseInt(args[4]);
		final int num_pending = Integer.parseInt(args[5]);

		if ("true".equals(System.getProperty("localmode"))) {
			MkClusterParam mkClusterParam = new MkClusterParam();
			mkClusterParam.setSupervisors(1);
			mkClusterParam.setPortsPerSupervisor(1);

			Testing.withLocalCluster(mkClusterParam, new TestJob() {

				@Override
				public void run(ILocalCluster cluster) throws Exception {

					// CassandraState.Options<String> options = new
					// CassandraState.Options<String>();
					// IStateFactory cassandra = new
					// CassandraState.Factory("localhost:9160", options );

					HashMap<String, String> hdfs = new HashMap<String, String>();
					hdfs.put("fs.default.name", "hdfs://localhost:9000");
					hdfs.put("dfs.replication", "1");
					IStateFactory cassandra = new HDFSState.Factory(hdfs);

					TopologyBuilder builder = new TopologyBuilder();
					RandomTreeGenerator stream = new RandomTreeGenerator();
					stream.prepareForUse();
					MOAStreamSpout moa_stream = new MOAStreamSpout(stream, 0);

					stream = new RandomTreeGenerator();
					stream.prepareForUse();
					MOAStreamSpout evaluate_stream = new MOAStreamSpout(stream,
							0);

					buildLearnPart(cassandra, moa_stream, builder,
							"trees.HoeffdingTree -m 1000000 -e 10000",
							num_workers, ensemble_size, num_classifiers);
					buildEvaluatePart(cassandra, evaluate_stream, builder,
							num_workers, ensemble_size, num_classifiers,
							num_classifiers, num_aggregators);
					builder.setBolt("calculate_performance", new CounterBolt(),
							num_workers).customGrouping("aggregate_result",
							new LocalGrouping(new IdBasedGrouping()));

					Config conf = new Config();
					conf.setNumAckers(num_workers);
					conf.setNumWorkers(num_workers);
					conf.setMaxSpoutPending(num_pending);
					cluster.submitTopology("test", conf,
							builder.createTopology());
					Thread.sleep(10000000);
				}

			});
		} else {

			conf.setNumAckers(num_workers);
			conf.setNumWorkers(num_workers);
			conf.setMaxSpoutPending(num_pending);
			conf.put("topology.worker.childopts",
					"-javaagent:/research/vp37/storm-0.8.2-wip8/lib/sizeofag-1.0.0.jar");
			conf.put("topology.message.timeout.secs", 60);

			// CassandraState.Options<String> options = new
			// CassandraState.Options<String>();
			// IStateFactory cassandra = new
			// CassandraState.Factory("ml64-1:9160", options );

			HashMap<String, String> hdfs = new HashMap<String, String>();
			hdfs.put("fs.default.name", "hdfs://ml64-1:9000");
			hdfs.put("dfs.replication", "1");
			IStateFactory cassandra = new HDFSState.Factory(hdfs);

			TopologyBuilder builder = new TopologyBuilder();
			RandomTreeGenerator stream = new RandomTreeGenerator();
			stream.prepareForUse();
			MOAStreamSpout moa_stream = new MOAStreamSpout(stream, 100);

			stream = new RandomTreeGenerator();
			stream.prepareForUse();
			MOAStreamSpout evaluate_stream = new MOAStreamSpout(stream, 0);
			if ("-lol".equals(args[6])) {

				buildLearnPart(cassandra, moa_stream, builder,
						"trees.HoeffdingTree -m 10000000 -e 10000",
						num_workers, ensemble_size, num_classifiers);

				StormSubmitter.submitTopology(
						"learn" + System.currentTimeMillis(), conf,
						builder.createTopology());
			}

			builder = new TopologyBuilder();
			buildEvaluatePart(cassandra, evaluate_stream, builder, num_workers,
					ensemble_size, num_classifiers, num_classifiers,
					num_aggregators);
			builder.setBolt("calculate_performance", new CounterBolt(),
					num_workers).customGrouping("aggregate_result",
					new LocalGrouping(new IdBasedGrouping()));

			StormSubmitter.submitTopology(
					"evaluate" + System.currentTimeMillis(), conf,
					builder.createTopology());

		}
	}

	public static void main(String[] args) throws Throwable {

		Config conf = new Config();
		ClonedStorm storm = new ClonedStorm(conf, args);
	}

}
