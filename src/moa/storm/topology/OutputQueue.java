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
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class OutputQueue implements Aggregator {

	
	private transient Publisher m_publisher;
	private Properties m_config;
	
	public OutputQueue(Properties config)
	{
		m_config = config;
	}

	
	
	class Publisher implements Runnable
	{
		private transient com.rabbitmq.client.Connection amqpConnection;
		private transient com.rabbitmq.client.Channel ampqChannel;
		private Properties m_config;
		
		public BlockingQueue<Object> m_output_queue = new LinkedBlockingQueue<Object>();
		
		public Publisher(Properties config)
		{
			m_config = config;
		}
		
		public void add(Object o)
		{
			try {
				m_output_queue.put(o);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		public void run() {
			setup();
			Object res;
			while (true)
			{
				try {
					res = m_output_queue.poll(10000, TimeUnit.HOURS);
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream os = new ObjectOutputStream(bos);
				os.writeObject(res);
				os.close();
				ampqChannel.basicPublish(m_config.getProperty("ampq.prediction_results_exchange"), "",
						null, bos.toByteArray());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}
			
		}
	}
	
	/** 
	 * Establish connection to the output queue. 
	 */
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		m_publisher = new Publisher(m_config);
		new Thread(m_publisher).start();
	}

	@Override
	public void cleanup() {
	}
	


	@Override
	public Object init(Object batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void aggregate(Object val, TridentTuple tuple,
			TridentCollector collector) {
		Object prediction = tuple.getValueByField(LearnEvaluateTopology.FIELD_PREDICTION);
		Object instance = tuple.getValueByField(LearnEvaluateTopology.FIELD_INSTANCE);
		ArrayList<Object> r = new ArrayList<Object>();
		r.add( prediction );
		r.add(instance);
		m_publisher.add(r);
	}

	@Override
	public void complete(Object val, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

}
