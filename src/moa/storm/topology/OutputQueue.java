package moa.storm.topology;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class OutputQueue implements Filter {

	private com.rabbitmq.client.Connection amqpConnection;
	private com.rabbitmq.client.Channel ampqChannel;
	private Properties m_config;
	
	public OutputQueue(Properties config)
	{
		m_config = config;
	}

	/** 
	 * Establish connection to the output queue. 
	 */
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
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
					.exchangeDeclare(m_config.getProperty("ampq.prediction_results_exchange"), "fanout");
	
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

	@Override
	public void cleanup() {
		try {
			amqpConnection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
		ampqChannel.basicPublish(m_config.getProperty("ampq.exchange"), "",
				null, bos.toByteArray());
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		List<Object> values = tuple.getValues();
		try {
			send(values);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
