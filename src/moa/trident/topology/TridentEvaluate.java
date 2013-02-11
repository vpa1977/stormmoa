package moa.trident.topology;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Properties;

import moa.classifiers.Classifier;
import moa.core.ObjectRepository;
import moa.options.ClassOption;
import moa.options.FlagOption;
import moa.options.IntOption;
import moa.streams.InstanceStream;
import moa.tasks.MainTask;
import moa.tasks.TaskMonitor;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TridentEvaluate extends MainTask {

	private Connection amqpConnection;
	private Channel learningChannel;
	private Channel predictionChannel;
	private Channel predictionResultChannel;
	
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
			learningChannel = createChannel(m_config.getProperty("ampq.exchange"), m_config.getProperty("ampq.learning_queue"));
			predictionChannel = createChannel(m_config.getProperty("ampq.prediction_exchange"), m_config.getProperty("ampq.prediction_queue"));
			//predictionResultChannel = createChannel(m_config.getProperty("ampq.prediction_results_exchange"), m_config.getProperty("ampq.prediction_results_queue"));
			
		} catch (Throwable t) {
			t.printStackTrace();
			throw new RuntimeException("Ampq failed to connect");
		}

	}

	private Channel createChannel(String exchangeName, String queueName) throws IOException {
		Channel channel = amqpConnection.createChannel();
		channel.exchangeDeclare(exchangeName,
				"fanout");

		final Queue.DeclareOk queue = channel.queueDeclare(
				queueName,
				/* durable */true,
				/* non-exclusive */false,
				/* non-auto-delete */false,
				/* no arguments */null);

		channel.queueBind(queue.getQueue(),
			exchangeName, "#");
		return channel;
	}

	/**
	 * Send next instance to the cluster.
	 * 
	 * @param inst
	 * @throws IOException
	 */
	public void send(String exchange, Channel c, Object data) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bos);
		os.writeObject(data);
		os.close();
		c.basicPublish(exchange, "",
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
			InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
			monitor.setCurrentActivity("Connecting to AMQP Broker", 1.0);
			monitor.setCurrentActivityFractionComplete(0);
			connect();
			
			String r;
			long currentProcessed =0;
			
			int maxInstances = this.maxInstancesOption.getValue();
			int numPasses = this.numPassesOption.getValue();
			long start = System.currentTimeMillis();
			for (int pass = 0; pass < numPasses; pass++) {
				long instancesProcessed = 0;
				monitor.setCurrentActivity("Sending for evaluation"
						+ (numPasses > 1 ? (" (pass " + (pass + 1) + "/"
								+ numPasses + ")") : "") + "...", -1.0);
				if (pass > 0) {
					stream.restart();
				}
				while (stream.hasMoreInstances()
						&& ((maxInstances < 0) || (instancesProcessed < maxInstances))) {

					send(m_config.getProperty("ampq.prediction_exchange"), predictionChannel, stream.nextInstance());

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
					}
				}

				monitor.setCurrentActivity(
						"Waiting for cluster to finish processing", 1.0);
				monitor.setCurrentActivityFractionComplete(0);
				//predictionResultChannel.basicConsume(m_, callback)

			}
			long end  = System.currentTimeMillis();
			System.out.println ("Done in "+ (end -start) + " ms "); 
			// - send instances into queue
			monitor.setCurrentActivity("Retrieving classifier", 1.0);

			// + query number of instances processed
			// - query number of instances processed

			return null;
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
