package moa.storm.topology;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;

import storm.trident.operation.Aggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class OutputQueue implements Function {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3321348386133534268L;
	private transient com.rabbitmq.client.Connection amqpConnection;
	private transient com.rabbitmq.client.Channel ampqChannel;
	private Properties m_config;
	
	public OutputQueue(Properties config)
	{
		m_config = config;
	}

	
	
	public void setup()
	{
		try {
			final ConnectionFactory connectionFactory = new ConnectionFactory();
	
			connectionFactory.setHost(m_config.getProperty("ampq.host"));
			connectionFactory.setPort(Integer.parseInt(m_config.getProperty("ampq.port")));
			connectionFactory.setUsername(m_config.getProperty("ampq.username"));
			if (m_config.getProperty("ampq.password").length() > 0)
				connectionFactory.setPassword(m_config.getProperty("ampq.password"));
			else
				connectionFactory.setPassword(null);
			
			connectionFactory.setVirtualHost(m_config.getProperty("ampq.vhost"));
	
			amqpConnection = connectionFactory.newConnection();
			ampqChannel = amqpConnection.createChannel();
			ampqChannel
					.exchangeDeclare(m_config.getProperty("ampq.prediction_results_exchange"), "topic");
	
			final Queue.DeclareOk queue = ampqChannel.queueDeclare(
					m_config.getProperty("ampq.prediction_results_queue"),
					/* durable */true,
					/* non-exclusive */false,
					/* non-auto-delete */false,
					/* no arguments */null);
	
			ampqChannel.queueBind(queue.getQueue(),
					m_config.getProperty("ampq.prediction_results_exchange"), "#");
		}
		catch (Throwable t ){
			t.printStackTrace();
			throw new RuntimeException("Ampq failed to connect");
		}

	}
	
	/** 
	 * Establish connection to the output queue. 
	 */
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		setup();
	}

	@Override
	public void cleanup() {
	}
	

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Object prediction = tuple.getValueByField(LearnEvaluateTopology.FIELD_PREDICTION);
		Object instance = tuple.getValueByField(LearnEvaluateTopology.FIELD_INSTANCE);
		ArrayList<Object> r = new ArrayList<Object>();
		r.add( prediction );
		r.add(instance);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(bos);
		os.writeObject(r);
		os.close();
		
		ampqChannel.basicPublish(m_config.getProperty("ampq.prediction_results_exchange"), "",
				 null,bos.toByteArray());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
