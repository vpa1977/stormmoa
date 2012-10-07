package moa.storm.topology;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.spout.AMQPSpout;

import moa.classifiers.Classifier;
import moa.options.ClassOption;
import moa.options.Option;
import moa.options.Options;
import moa.storm.scheme.InstanceScheme;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Filter;
import storm.trident.state.StateFactory;
import trident.memcached.MemcachedState;
import backtype.storm.LocalDRPC;

public class StormClusterTopology  extends LearnEvaluateTopology {
	
	Properties m_config;
	
	
	public StormClusterTopology() throws IOException
	{
		Properties prp = new Properties();
		prp.load(getClass().getResourceAsStream("/storm_cluster.properties"));
		m_config = prp;
		
	}

	@Override
	public Filter outputQueue(Map options) {
		return new OutputQueue();
	}

	@Override
	public Stream createEvaluationStream(Map options, TridentTopology topology) {
		SharedQueueWithBinding queue = new SharedQueueWithBinding(m_config.getProperty("ampq.evaluation_queue"), 
				m_config.getProperty("ampq.exchange"), "#");
		String host = m_config.getProperty("ampq.host");
		int port = Integer.parseInt( m_config.getProperty("ampq.port")); // default ampq host
		String vhost = m_config.getProperty("ampq.vhost");
		String username =m_config.getProperty("ampq.username");
		String password =m_config.getProperty("ampq.password");
		InstanceScheme scheme = new InstanceScheme();

		AMQPSpout spout = new AMQPSpout(host, port, username, password,
				vhost, queue, scheme);

		return topology.newStream("instances", spout);
	}

	@Override
	public Stream createLearningStream(Map options, TridentTopology topology) {
		SharedQueueWithBinding queue = new SharedQueueWithBinding(m_config.getProperty("ampq.learning_queue"), 
				m_config.getProperty("ampq.exchange"), "#");
		String host = m_config.getProperty("ampq.host");
		int port = Integer.parseInt( m_config.getProperty("ampq.port")); // default ampq host
		String vhost = m_config.getProperty("ampq.vhost");
		String username =m_config.getProperty("ampq.username");
		String password =m_config.getProperty("ampq.password");
		InstanceScheme scheme = new InstanceScheme();
		AMQPSpout spout = new AMQPSpout(host, port, username, password,
				vhost, queue, scheme);
		return topology.newStream("instances", spout);
	}

	@Override
	public LocalDRPC getDRPC(Map options) {
		return null;
	}

	@Override
	public StateFactory createFactory(Map options) {
		ArrayList<InetSocketAddress> memcachedHosts = new ArrayList<InetSocketAddress>();
		Enumeration<?> en = m_config.propertyNames();
		while (en.hasMoreElements())
		{
			String name = String.valueOf(en.nextElement());
			if (name.startsWith("memcached"))
			{
				String value = m_config.getProperty(name);
				StringTokenizer tk = new StringTokenizer( value, ":");
				String host = tk.nextToken();
				int port = Integer.parseInt(tk.nextToken());
				InetSocketAddress addr = new InetSocketAddress(host,port);
				memcachedHosts.add( addr);
			}
		}
		return MemcachedState.transactional(memcachedHosts);
		
	}

	@Override
	public Classifier getClassifier(Map options) {
		String cliString = m_config.getProperty("moa.classifier");
		try {
			return (Classifier)ClassOption.cliStringToObject(cliString, Classifier.class, new Option[]{});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
