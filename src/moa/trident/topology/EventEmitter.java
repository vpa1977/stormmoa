package moa.trident.topology;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import moa.classifiers.Classifier;
import moa.core.Measurement;
import moa.core.ObjectRepository;
import moa.core.TimingUtils;
import moa.evaluation.ClassificationPerformanceEvaluator;
import moa.evaluation.LearningCurve;
import moa.evaluation.LearningEvaluation;
import moa.options.ClassOption;
import moa.options.IntOption;
import moa.options.StringOption;
import moa.streams.InstanceStream;
import moa.tasks.MainTask;
import moa.tasks.TaskMonitor;

import org.apache.commons.io.output.ByteArrayOutputStream;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Debug;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.spout.AMQPSpout;

/**
 * Sends MOA Data Stream events to the Storm Cluster.
 * 
 * @author bsp
 * 
 */
public class EventEmitter extends MainTask {
	
	
	
	private static void startLocalMemcacheInstance(int port) {
        
    
    }
	
	class MOAQueryFunction extends BaseQueryFunction<ReadOnlySnapshottable<Long>, Long> implements Serializable
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;



		public void execute(TridentTuple tuple, Long result,
				TridentCollector collector) {
			ArrayList<Object> out = new ArrayList<Object>();
			out.add(result);
			collector.emit(out);
		}

		public List<Long> batchRetrieve(ReadOnlySnapshottable<Long> state,
				List<TridentTuple> args) {
			Long lol = state.get();
			if (lol == null)
				lol = new Long(0);
			ArrayList<Long> list = new ArrayList<Long>();
			for (TridentTuple t : args)
			{
				list.add( new Long(lol));
			}
			return list;
		}
		
	}

	class EventProcessor implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public EventProcessor(LocalDRPC drpc) {
			
			LocalCluster cluster = new LocalCluster();
			int PORT = 10001;
	        startLocalMemcacheInstance(PORT);
	        StateFactory memcached = null;//MemcachedState.nonTransactional(Arrays.asList(new InetSocketAddress("localhost", PORT)));
	        
			Config conf = new Config();
			// conf.setDebug(true);
			conf.setMaxTaskParallelism(1);
			//conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 1);
			
			TridentTopology topology = new TridentTopology();
			

			SharedQueueWithBinding queue = new SharedQueueWithBinding(
					"moa-events", "moa", "#");
			String host = "localhost";
			int port = 5672; // default ampq host
			String vhost = "/";
			String username = "guest";
			String password = "test";
			InstanceScheme scheme = new InstanceScheme();

			AMQPSpout spout = new AMQPSpout(host, port, username, password,
					vhost, queue, scheme);

			Stream instanceStream = topology.newStream("instances", spout);
			
			instanceStream = instanceStream.each(scheme.getOutputFields(),
					new Debug());
			
		/*	GroupedStream grpStream = instanceStream.groupBy(new Fields("instance_tag"));
			IAggregatableStream grpStream2 = grpStream.each(scheme.getOutputFields(), new Function(){

				public void prepare(Map conf, TridentOperationContext context) {
					// TODO Auto-generated method stub
					
				}

				public void cleanup() {
					// TODO Auto-generated method stub
					
				}

				public void execute(TridentTuple tuple,
						TridentCollector collector) {
					
					collector.emit(tuple.getValues());
					
				}
				
			}, new Fields() );*/
			
			EvaluationAggregator evaluator = new EvaluationAggregator(
					evaluatorOption, learnerOption, streamOption);

			TridentState performanceMetrics = instanceStream
					.persistentAggregate(memcached, new Fields("instance"), evaluator, new Fields("accuracy"));

			performanceMetrics.parallelismHint(1);

			Stream queryStream = topology.newDRPCStream("stats", drpc);
			queryStream.stateQuery(performanceMetrics, new MOAQueryFunction(), new Fields("accuracy"));

			cluster.submitTopology("moatest", conf, topology.build());
		}
	}

	class EvaluationAggregator implements
			ReducerAggregator<Long> {
		private ClassOption m_option;
		private ClassOption learnerOption;
		private ClassOption streamOption;

		private Classifier learner;
		private LearningCurve learningCurve;
		private ClassificationPerformanceEvaluator evaluator;

		public EvaluationAggregator(ClassOption eval, ClassOption l,
				ClassOption stream) {
			m_option = eval;
			learnerOption = l;
			streamOption = stream;
		}

		public Long init() {
			learningCurve = new LearningCurve("learning evaluation instances");
			learner = (Classifier) getPreparedClassOption(learnerOption);
			InstanceStream stream = (InstanceStream) getPreparedClassOption(streamOption);
			learner.setModelContext(stream.getHeader());
			evaluator =(ClassificationPerformanceEvaluator) getPreparedClassOption(m_option); 
			return 1L;
		}

		public Long reduce(
				Long curr, TridentTuple tuple) {
			if (curr == null)
				curr = init();

			Instance trainInst = (Instance) tuple.getValue(0);

			Instance testInst = (Instance) trainInst.copy();
			int trueClass = (int) trainInst.classValue();
			// testInst.setClassMissing();
			double[] prediction = learner.getVotesForInstance(testInst);
			evaluator.addResult(testInst, prediction);
			learner.trainOnInstance(trainInst);
			return curr + 1;
		}

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1660816488216975501L;

	@Override
	public String getPurposeString() {
		return "Submits events to the AMQP Exchange.";
	}

	public IntOption generateSizeOption = new IntOption("generateSize", 'g',
			"Number of examples.", 10000000, 0, Integer.MAX_VALUE);

	public ClassOption streamOption = new ClassOption("stream", 's',
			"Stream to evaluate on.", InstanceStream.class,
			"generators.RandomTreeGenerator");

	public StringOption amqpHostOption = new StringOption("ampqHost", 'h',
			"AMQP Host", "localhost");

	public IntOption amqpPortOption = new IntOption("amqpPort", 'i',
			"AMQP Port", 5672, 0, Integer.MAX_VALUE);

	public StringOption amqpUsernameOption = new StringOption("amqpUsername",
			'u', "AMQP Username", "guest");

	public StringOption amqpPasswordOption = new StringOption("amqpPassword",
			'p', "AMQP Password", "test");

	public StringOption amqpVHostOption = new StringOption("amqpVHost", 'v',
			"AMQP Virtual Host", "/");

	public StringOption amqpExchangeOption = new StringOption("amqpExchange",
			'e', "AMQP Exchange", "moa");

	public StringOption amqpQueueOption = new StringOption("amqpQueue", 'q',
			"AMQP Exchange", "moa-events");

	public ClassOption evaluatorOption = new ClassOption("evaluator", 'E',
			"Classification performance evaluation method.",
			ClassificationPerformanceEvaluator.class,
			"WindowClassificationPerformanceEvaluator");

	public ClassOption learnerOption = new ClassOption("learner", 'l',
			"Classifier to train.", Classifier.class, "bayes.NaiveBayes");

	private com.rabbitmq.client.Connection amqpConnection;

	private Channel amqpChannel;

	public Class<?> getTaskResultType() {
		return LearningEvaluation.class;
	}

	@Override
	protected Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {

		monitor.setCurrentActivity("Connecting to AMQP Broker", 1.0);
		monitor.setCurrentActivityFractionComplete(0);
		LocalDRPC drpc = new LocalDRPC();

		EventProcessor processor = new EventProcessor(drpc);

		try {
			TimingUtils.enablePreciseTiming();

			final ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.setHost(amqpHostOption.getValue());
			connectionFactory.setPort(amqpPortOption.getValue());
			connectionFactory.setUsername(amqpUsernameOption.getValue());
			if (amqpPasswordOption.getValue().length() > 0)
				connectionFactory.setPassword(amqpPasswordOption.getValue());
			else
				connectionFactory.setPassword(null);
			connectionFactory.setVirtualHost(amqpVHostOption.getValue());

			amqpConnection = connectionFactory.newConnection();
			amqpChannel = amqpConnection.createChannel();
			amqpChannel
					.exchangeDeclare(amqpExchangeOption.getValue(), "fanout");

			final Queue.DeclareOk queue = amqpChannel.queueDeclare(
					amqpQueueOption.getValue(),
					/* durable */true,
					/* non-exclusive */false,
					/* non-auto-delete */false,
					/* no arguments */null);

			amqpChannel.queueBind(queue.getQueue(),
					amqpExchangeOption.getValue(), "#");

			int numInstances = 0;
			InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
			long genStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread();
			monitor.setCurrentActivity("Sending Stream",
					generateSizeOption.getValue());
			while (numInstances < this.generateSizeOption.getValue()) {
				Instance instance = stream.nextInstance();
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream os = new ObjectOutputStream(bos);
				os.writeObject(instance);
				os.close();
				amqpChannel.basicPublish(amqpExchangeOption.getValue(), "",
						null, bos.toByteArray());

				numInstances++;

				if (numInstances % (INSTANCES_BETWEEN_MONITOR_UPDATES*100) == 0) {
					
					String result = drpc.execute("stats", "");
					while (result.indexOf("null") != -1 )
					{
						result = drpc.execute("stats", "");
					}

					
					if (monitor.taskShouldAbort()) {
						return null;
					}
					long estimatedRemainingInstances = stream
							.estimatedRemainingInstances();
					if (this.generateSizeOption.getValue() > 0) {
						long maxRemaining = this.generateSizeOption.getValue()
								- numInstances;
						if ((estimatedRemainingInstances < 0)
								|| (maxRemaining < estimatedRemainingInstances)) {
							estimatedRemainingInstances = maxRemaining;
						}
					}
					monitor.setCurrentActivityFractionComplete(estimatedRemainingInstances < 0 ? -1.0
							: (double) numInstances
									/ (double) (numInstances + estimatedRemainingInstances));
					if (monitor.resultPreviewRequested()) {
						monitor.setLatestResultPreview("");
					}
				}

			}
			double genTime = TimingUtils.nanoTimeToSeconds(TimingUtils
					.getNanoCPUTimeOfCurrentThread() - genStartTime);
			return new LearningEvaluation(new Measurement[] {
					new Measurement("Number of instances generated",
							numInstances),
					new Measurement("Time elapsed", genTime),
					new Measurement("Instances per second", numInstances
							/ genTime) });
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
