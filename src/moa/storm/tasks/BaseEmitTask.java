package moa.storm.tasks;

import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.spout.AMQPSpout;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import weka.core.Instance;
import moa.options.ClassOption;
import moa.options.IntOption;
import moa.options.StringOption;
import moa.storm.scheme.InstanceScheme;
import moa.streams.InstanceStream;
import moa.tasks.MainTask;

public abstract class BaseEmitTask extends MainTask {
	
	@Override
	public String getPurposeString() {
		return "Submits events to the AMQP Exchange.";
	}

	public ClassOption streamOption = new ClassOption("stream", 's',
			"Stream to evaluate on.", InstanceStream.class,
			"generators.RandomTreeGenerator");

	public StringOption amqpHostOption = new StringOption("ampqHost", 'h',
			"AMQP Host", "localhost");

	public IntOption amqpPortOption = new IntOption("amqpPort", 'I',
			"AMQP Port", 5672, 0, Integer.MAX_VALUE);

	public StringOption amqpUsernameOption = new StringOption("amqpUsername",
			'U', "AMQP Username", "guest");

	public StringOption amqpPasswordOption = new StringOption("amqpPassword",
			'P', "AMQP Password", "test");

	public StringOption amqpVHostOption = new StringOption("amqpVHost", 'v',
			"AMQP Virtual Host", "/");

	public StringOption amqpExchangeOption = new StringOption("amqpExchange",
			'E', "AMQP Exchange", "moa");

	public StringOption amqpQueueOption = new StringOption("amqpQueue", 'Q',
			"AMQP Exchange", "moa-events");

	private com.rabbitmq.client.Connection amqpConnection;

	private Channel amqpChannel;
	
	
	protected Channel getChannel()
	{
		return amqpChannel;
	}
	
	/** 
	 * Connects MOA client to the AMQP exchange
	 * @throws IOException
	 */
	protected void connect() throws IOException
	{
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
	}
	
	
	/** 
	 * Send next instance to the cluster.
	 * @param inst
	 * @throws IOException 
	 */
	public void send(Object data) throws IOException
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bos);
		os.writeObject(data);
		os.close();
		amqpChannel.basicPublish(amqpExchangeOption.getValue(), "",
				null, bos.toByteArray());
	}
	
	/** 
	 * creates stream from the AMPQ spout
	 * @TODO remove hardcodes.
	 * @param topology
	 * @return
	 */
	protected Stream createStream(String name, TridentTopology topology)
	{
		SharedQueueWithBinding queue = new SharedQueueWithBinding(
				name, "moa", "#");
		String host = "localhost";
		int port = 5672; // default ampq host
		String vhost = "/";
		String username = "guest";
		String password = "test";
		InstanceScheme scheme = new InstanceScheme();

		AMQPSpout spout = new AMQPSpout(host, port, username, password,
				vhost, queue, scheme);

		return topology.newStream("instances", spout);
	}
}
