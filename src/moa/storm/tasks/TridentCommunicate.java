package moa.storm.tasks;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.apache.thrift7.TException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;

import weka.core.Instance;
import weka.core.Utils;
import moa.classifiers.Classifier;
import moa.core.ObjectRepository;
import moa.evaluation.ClassificationPerformanceEvaluator;
import moa.evaluation.LearningEvaluation;
import moa.options.ClassOption;
import moa.options.FileOption;
import moa.options.FlagOption;
import moa.options.IntOption;
import moa.options.StringOption;
import moa.streams.InstanceStream;
import moa.tasks.MainTask;
import moa.tasks.TaskMonitor;

public class TridentCommunicate extends MainTask {

	private Connection amqpConnection;
	private Channel ampqChannel;
	private Properties m_config;

	public ClassOption streamOption = new ClassOption("stream", 's',
			"Stream to evaluate on.", InstanceStream.class,
			"generators.RandomTreeGenerator");

	public IntOption numPassesOption = new IntOption("numPasses", 'p',
			"The number of passes to do over the data.", 1, 1,
			Integer.MAX_VALUE);

	@Override
	public String getPurposeString() {
		return "Communicates with the Storm Cluster..";
	}

	private static final long serialVersionUID = 1L;

	public IntOption maxInstancesOption = new IntOption("maxInstances", 'i',
			"Maximum number of instances to test.", 10000, 0,
			Integer.MAX_VALUE);

	public FlagOption learningOption = new FlagOption("isLearning", 'l',
			"Queue to use");

	public Class<?> getTaskResultType() {
		return Classifier.class;
	}

	private void connect() {
		try {
			final ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.setHost(m_config.getProperty("ampq.host"));
			connectionFactory.setPort(Integer.parseInt(m_config
					.getProperty("ampq.port")));
			connectionFactory
					.setUsername(m_config.getProperty("ampq.username"));
			if (m_config.getProperty("ampq.password").length() > 0)
				connectionFactory.setPassword(m_config
						.getProperty("ampq.password"));
			else
				connectionFactory.setPassword(null);

			connectionFactory
					.setVirtualHost(m_config.getProperty("ampq.vhost"));

			amqpConnection = connectionFactory.newConnection();
			ampqChannel = amqpConnection.createChannel();
			ampqChannel.exchangeDeclare(m_config.getProperty("ampq.exchange"),
					"fanout");

			final Queue.DeclareOk queue = ampqChannel.queueDeclare(
					m_config.getProperty("ampq.learning_queue"),
					/* durable */true,
					/* non-exclusive */false,
					/* non-auto-delete */false,
					/* no arguments */null);

			ampqChannel.queueBind(queue.getQueue(),
					m_config.getProperty("ampq.exchange"), "#");
		} catch (Throwable t) {
			t.printStackTrace();
			throw new RuntimeException("Ampq failed to connect");
		}

	}

	/**
	 * Send next instance to the cluster.
	 * 
	 * @param inst
	 * @throws IOException
	 */
	public void send(Object data) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bos);
		os.writeObject(data);
		os.close();
		ampqChannel.basicPublish(m_config.getProperty("ampq.exchange"), "",
				null, bos.toByteArray());
	}

	@Override
	protected Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {
		try {
			m_config = new Properties();

			try {
				m_config.load(getClass().getResourceAsStream(
						"/ml_storm_cluster.properties"));
			} catch (IOException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
			DRPCClient drpc = new DRPCClient(
					m_config.getProperty("storm.drpc_host"),
					Integer.parseInt(m_config.getProperty("storm.drpc_port")));
			InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
			monitor.setCurrentActivity("Connecting to AMQP Broker", 1.0);
			monitor.setCurrentActivityFractionComplete(0);
			connect();
			
		//	String r = drpc.execute("stats", "");
//			r = r.substring(5);
			//r = r.substring(0, r.length() - 2);
			//long currentProcessed = Long.parseLong(r);
			String r;
			long currentProcessed =0;
			
			int maxInstances = this.maxInstancesOption.getValue();
			int numPasses = this.numPassesOption.getValue();
			long start = System.currentTimeMillis();
			for (int pass = 0; pass < numPasses; pass++) {
				long instancesProcessed = 0;
				monitor.setCurrentActivity("Training learner"
						+ (numPasses > 1 ? (" (pass " + (pass + 1) + "/"
								+ numPasses + ")") : "") + "...", -1.0);
				if (pass > 0) {
					stream.restart();
				}
				while (stream.hasMoreInstances()
						&& ((maxInstances < 0) || (instancesProcessed < maxInstances))) {

					send(stream.nextInstance().copy());

					instancesProcessed++;
					if (instancesProcessed % INSTANCES_BETWEEN_MONITOR_UPDATES == 0) {
						if (monitor.taskShouldAbort()) {
							return null;
						}
						long estimatedRemainingInstances = stream
								.estimatedRemainingInstances();
						if (maxInstances > 0) {
							long maxRemaining = maxInstances
									- instancesProcessed;
							if ((estimatedRemainingInstances < 0)
									|| (maxRemaining < estimatedRemainingInstances)) {
								estimatedRemainingInstances = maxRemaining;
							}
						}
						monitor.setCurrentActivityFractionComplete(estimatedRemainingInstances < 0 ? -1.0
								: (double) instancesProcessed
										/ (double) (instancesProcessed + estimatedRemainingInstances));
						// if (monitor.resultPreviewRequested()) {
						// monitor.setLatestResultPreview(learner.copy());
						// }
					}
				}

				monitor.setCurrentActivity(
						"Waiting for cluster to finish processing", 1.0);
				monitor.setCurrentActivityFractionComplete(0);
				waitCluster(currentProcessed, monitor, drpc, instancesProcessed);

			}
			long end  = System.currentTimeMillis();
			System.out.println ("Done in "+ (end -start) + " ms "); 
			// - send instances into queue
			monitor.setCurrentActivity("Retrieving classifier", 1.0);

			// + query number of instances processed
			// - query number of instances processed

			// query the classifer
			r = drpc.execute("classifier", "");
			// skip to the data
			r = r.substring(6);
			byte[] b = DatatypeConverter.parseBase64Binary(r);
			ObjectInputStream is = new ObjectInputStream(
					new ByteArrayInputStream(b));

			Classifier cls = (Classifier) is.readObject();
			System.out.println(cls);
			return cls;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	protected long waitCluster(long currentProcessed, TaskMonitor monitor, DRPCClient drpc,
			long instancesProcessed) throws TException, DRPCExecutionException {
		long clusterProcessed = 0;
		while (clusterProcessed < instancesProcessed + currentProcessed) {
			String r = drpc.execute("stats", "");
			r = r.substring(5);
			r = r.substring(0, r.length() - 2);
			clusterProcessed = Long.parseLong(r);
			monitor.setCurrentActivityFractionComplete((double) clusterProcessed
					/ (double) (instancesProcessed));
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return clusterProcessed;
	}

	private void waitClusterModel(DRPCClient drpc) throws TException,
			DRPCExecutionException {
		String result = "";
		while ("".equals(result) || ("[[\"\",\"\"]]".equals(result))) {
			result = drpc.execute("classifier", "");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
