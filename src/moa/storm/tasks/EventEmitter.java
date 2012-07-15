package moa.storm.tasks;


import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;

import weka.core.Instance;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;

import moa.core.Measurement;
import moa.core.ObjectRepository;
import moa.core.TimingUtils;
import moa.evaluation.LearningEvaluation;
import moa.options.ClassOption;
import moa.options.IntOption;
import moa.options.StringOption;
import moa.streams.InstanceStream;
import moa.tasks.AbstractTask;
import moa.tasks.MainTask;
import moa.tasks.TaskMonitor;

/**
 * Sends MOA Data Stream events to the Storm Cluster.
 * @author bsp
 *
 */
public class EventEmitter extends MainTask {

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
    
    public StringOption amqpUsernameOption = new StringOption("amqpUsername", 'u', 
    		"AMQP Username", "guest");
    
    public StringOption amqpPasswordOption = new StringOption("amqpPassword", 'p', 
    		"AMQP Password", "test");

    public StringOption amqpVHostOption = new StringOption("amqpVHost", 'v', 
    		"AMQP Virtual Host", "/");
    
    public StringOption amqpExchangeOption = new StringOption("amqpExchange", 'e', "AMQP Exchange", "moa");
    
    public StringOption amqpQueueOption = new StringOption("amqpQueue", 'q', "AMQP Exchange", "moa-events");

	private com.rabbitmq.client.Connection amqpConnection;

	private Channel amqpChannel;

    @Override
    public Class<?> getTaskResultType() {
        return LearningEvaluation.class;
    }
	
	@Override
	protected Object doMainTask(TaskMonitor monitor, ObjectRepository repository) {
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
			amqpChannel.exchangeDeclare(amqpExchangeOption.getValue(), "fanout");

		    final Queue.DeclareOk queue = amqpChannel.queueDeclare(
		    		amqpQueueOption.getValue(),
		                /* durable */ true,
		                /* non-exclusive */ false,
		                /* non-auto-delete */ false,
		                /* no arguments */ null);

		    amqpChannel.queueBind(queue.getQueue(), amqpExchangeOption.getValue(), "#");
		  
	        int numInstances = 0;
	        InstanceStream stream = (InstanceStream) getPreparedClassOption(this.streamOption);
	        long genStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread();
	        while (numInstances < this.generateSizeOption.getValue()) {
	            Instance instance = stream.nextInstance();
	            ByteArrayOutputStream bos = new ByteArrayOutputStream();
	            ObjectOutputStream os = new ObjectOutputStream(bos);
	            os.writeObject(instance);
	            os.close();
	            amqpChannel.basicPublish(amqpExchangeOption.getValue(), "", null, bos.toByteArray());
	            
	            numInstances++;
	        }
	        double genTime = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread()
	                - genStartTime);
	        return new LearningEvaluation(
	                new Measurement[]{
	                    new Measurement("Number of instances generated",
	                    numInstances),
	                    new Measurement("Time elapsed", genTime),
	                    new Measurement("Instances per second", numInstances
	                    / genTime)});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
