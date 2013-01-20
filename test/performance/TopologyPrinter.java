package performance;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.jgrapht.DirectedGraph;

import storm.trident.operation.Function;
import storm.trident.planner.Node;
import storm.trident.planner.ProcessorNode;
import storm.trident.planner.SubtopologyBolt;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.processor.EachProcessor;
import storm.trident.spout.TridentSpoutCoordinator;
import storm.trident.spout.TridentSpoutExecutor;
import storm.trident.topology.TridentBoltExecutor;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.BasicBoltExecutor;

public class TopologyPrinter {
	
	Map<String, StreamInfo> global_stream_list = new HashMap<String,StreamInfo>();
	
	
	public void collectStreams(StormTopology result)
	{
		Map<String, Bolt> bolts = result.get_bolts();
		Iterator<String> it = bolts.keySet().iterator();
		while (it.hasNext())
		{
			String name = it.next();
			Bolt bolt = bolts.get(name);
			ComponentCommon common = bolt.get_common();
			Map<String,StreamInfo> streams = common.get_streams();
			Iterator<String> streamKeys =streams.keySet().iterator(); 
			while (streamKeys.hasNext())
			{
				String streamKey = streamKeys.next();
				StreamInfo info = streams.get(streamKey);
				global_stream_list.put(streamKey, info);
			}
		}
	}

	public void print(StormTopology result) throws Throwable {
		collectStreams(result);
		Map<String, Bolt> bolts = result.get_bolts();
		
		
		Iterator<String> it = bolts.keySet().iterator();
		while (it.hasNext())
		{
			String name = it.next();
			Bolt bolt = bolts.get(name);
			ComponentObject component = bolt.get_bolt_object();
			ComponentCommon common = bolt.get_common();
			System.out.println("Bolt "+ name + " parallel "+ common.get_parallelism_hint());
			Map<GlobalStreamId, Grouping> inputs = common.get_inputs();
			Iterator<GlobalStreamId> inputKeys = inputs.keySet().iterator();
			System.out.println("\tInputs:");
			while (inputKeys.hasNext())
			{
				GlobalStreamId next = inputKeys.next();
				Grouping grp = inputs.get(next);
				String grpText = grp.toString();
				if (grp.is_set_custom_serialized()) 
					grpText = "<Grouping custom_serialized>";
				String from = next.get_componentId();
				String stream = next.get_streamId();
				StreamInfo info = global_stream_list.get(stream);
				String fields = "[]";
				if (info != null)
				{
					fields = info.get_output_fields().toString();
				}
				System.out.println("\t  ("+stream +") " + from + "=>"+fields + " < "+ grpText);
			}
			System.out.println("\tOutputs:");
			Map<String,StreamInfo> streams = common.get_streams();
			Iterator<String> streamKeys =streams.keySet().iterator(); 
			while (streamKeys.hasNext())
			{
				String streamKey = streamKeys.next();
				StreamInfo info = streams.get(streamKey);
				
				System.out.println("\t  (" + streamKey + ") "+ info.get_output_fields());
			}
			
			byte[] data  = component.get_serialized_java();	
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
			Object obj = ois.readObject();
			if (obj instanceof TridentBoltExecutor || obj instanceof BasicBoltExecutor)  {
				
				Field boltField = obj.getClass().getDeclaredField("_bolt");
				boltField.setAccessible(true);
				Object subtopology = boltField.get(obj);
				if (subtopology instanceof TridentSpoutExecutor || subtopology instanceof TridentSpoutCoordinator )
				{
					Field f = subtopology.getClass().getDeclaredField("_spout");
					f.setAccessible(true);
					Object spField = f.get(subtopology);
					f = spField.getClass().getDeclaredField("_spout");
					f.setAccessible(true);
					spField = f.get(spField);
					System.out.println(" "+spField);
				}
				else
				{
					Field nodesField = subtopology.getClass().getDeclaredField("_nodes");
					nodesField.setAccessible(true);
					Object nodes = nodesField.get( subtopology);
					HashSet<Node> planner = (HashSet<Node>) nodes;
					ArrayList<Node> ar = new ArrayList<Node>(planner);
					Collections.sort(ar, new Comparator<Node>(){

						@Override
						public int compare(Node arg0, Node arg1) {
							return arg0.creationIndex - arg1.creationIndex;
						}
						
					});
					System.out.println(" "+ar);
					for (Node n : ar)
					{
						if (n instanceof ProcessorNode) {
							ProcessorNode proc = (ProcessorNode) n;
							TridentProcessor processor = proc.processor;
							if (processor instanceof EachProcessor )
							{
								Field f = processor.getClass().getDeclaredField("_function");
								f.setAccessible(true);
								Object function = f.get(proc.processor);
								System.out.println(" ["+ function + "] self: " + proc.selfOutFields);
							}
							else
							{
								System.out.println(" ["+ processor + "]");
							}
							
						} else {
							System.out.println("Break");
						}
					}
				}
				
				
				
			} else
			
			{
				
				System.out.println("Break");
			}
		}
		
	}

}
