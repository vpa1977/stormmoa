package moa.storm.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

import moa.classifiers.Classifier;
import moa.core.MiscUtils;
import moa.storm.tasks.BaggingLearnerAggregator;
import moa.storm.tasks.ClassifierQueryFunction;
import moa.storm.tasks.EvaluateQueryFunction;
import moa.storm.tasks.LearnerAggregator;
import moa.storm.tasks.LearnerWrapper;
import moa.storm.tasks.StatQueryFunction;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.Filter;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateSpec;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;
import weka.core.Instance;


/** 
 * Standard topology for learning and evaluating moa classifier
 * @author bsp
 *
 */
public abstract class LearnEvaluateTopology {
	

	
	
	private static final String FIELD_STATISTICS = "statistics";
	public static final String FIELD_PREDICTION = "prediction";
	public static final String FIELD_INSTANCE = "instance";
	public static final String RPC_STATS = "stats";
	public static final String RPC_EVALUATE = "evaluate";
	public static final String RPC_CLASSIFIER = "classifier";
	
	public static final String FIELD_CLASSIFIER = "classifier";
	public static final String BAGGING_ENSEMBLE_SIZE = "bagging.ensemble_size";
	private static final String FIELD_WEIGHT = "weight";
	
	public TridentTopology createOzaBag(Map options)
	{
		TridentTopology topology = new TridentTopology();
		
		LocalDRPC drpc = getDRPC(options);
		 
		
		int ensemble_size = Integer.parseInt(String.valueOf(options.get(BAGGING_ENSEMBLE_SIZE)));
		TridentState[] classifierState = new TridentState[ensemble_size];
		
		int par_learning = 4;
		String learning_parallelism = (String)options.get("learning.parallelism");
		if (learning_parallelism!= null)
			par_learning = Integer.parseInt(learning_parallelism);
 
		Stream learningStream = createLearningStream(options, topology).parallelismHint(par_learning).
				each(new Fields(FIELD_INSTANCE), new Deserialize(), new Fields());
		for (int i = 0; i < ensemble_size; i++)
		{
			
			classifierState[i] =learningStream.persistentAggregate(createFactory("key_classifier"+i), new Fields(FIELD_INSTANCE),
							new BaggingLearnerAggregator(new LearnerWrapper( getClassifier(options) )), new Fields(FIELD_CLASSIFIER+i));	
		}
		
		/*TridentState readCount = learningStream.persistentAggregate(createFactory(FIELD_CLASSIFIER+System.currentTimeMillis()), new ReducerAggregator<Long>(){

			@Override
			public Long init() {
				// TODO Auto-generated method stub
				return new Long(0);
			}

			@Override
			public Long reduce(Long curr, TridentTuple tuple) {
				if (curr == null)
					curr = init();
				return curr+1;
			}
			
		}, new Fields("count_instances"));
		
		topology.newDRPCStream(RPC_STATS, drpc).
				stateQuery(readCount,  new BaseQueryFunction<ReadOnlySnapshottable<Long>, Long>(){

					@Override
					public List<Long> batchRetrieve(
							ReadOnlySnapshottable<Long> state,
							List<TridentTuple> args) {
						ArrayList<Long> res = new ArrayList<Long>();
						res.add( state.get());
						return res;
					}

					@Override
					public void execute(TridentTuple tuple, Long result,
							TridentCollector collector) {
						ArrayList<Object> list = new ArrayList<Object>();
						if (result != null)
							list.add(result);
						else
							list.add( new Long(0));
						collector.emit(list);
						
					}
					
				}, new Fields("count_instances"));
		
		
		Stream evaluate = topology.newDRPCStream(RPC_EVALUATE, drpc);
		
		ArrayList<Stream> merge = new ArrayList<Stream>();
		for (int i = 0; i < ensemble_size; i++)
		{
			merge.add(evaluate.
					stateQuery(classifierState[i],new Fields("args"), 
								new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION, FIELD_INSTANCE)));
		}
		topology.merge(merge).groupBy(new Fields(FIELD_INSTANCE)).aggregate(new BaggingAggregator(),  new Fields(FIELD_PREDICTION));
		*/
		

		int par_evaluation = 4;
		String evaluation_parallelism = (String)options.get("evaluation.parallelism");
		if (evaluation_parallelism!= null)
			par_evaluation = Integer.parseInt(evaluation_parallelism);
		Stream predictionStream = createPredictionStream(options, topology).parallelismHint(par_evaluation);
		ArrayList<Stream> merge_queue = new ArrayList<Stream>();
		for (int i = 0; i < ensemble_size; i++)
		{
			merge_queue.add(predictionStream.
					stateQuery(classifierState[i],new Fields(FIELD_INSTANCE), 
								new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION)));
		}
		
		topology.merge(merge_queue).groupBy(new Fields(FIELD_INSTANCE)).aggregate(new Fields(FIELD_INSTANCE, FIELD_PREDICTION),new BaggingAggregator(),  new Fields(FIELD_PREDICTION))
		.each(new Fields(FIELD_INSTANCE,FIELD_PREDICTION), outputQueue(options));
		
		//aggregate(new Fields(FIELD_PREDICTION, FIELD_INSTANCE),outputQueue(options),new Fields(FIELD_PREDICTION, FIELD_INSTANCE));
			
			
		return topology;

	}

	public TridentTopology create(Map options)
	{
		TridentTopology topology = new TridentTopology();
		
		LocalDRPC drpc = getDRPC(options);
		; 
		LearnerWrapper wrapper = new LearnerWrapper( getClassifier(options) );
		
		int par_learning = 1;
		String learning_parallelism = (String)options.get("learning.parallelism");
		if (learning_parallelism!= null)
			par_learning = Integer.parseInt(learning_parallelism);
		
		Stream learningStream = createLearningStream(options, topology).parallelismHint(par_learning);
		TridentState classifierState = learningStream.persistentAggregate(createFactory(System.currentTimeMillis()+""), new Fields(FIELD_INSTANCE), 
				new LearnerAggregator(wrapper), new Fields(FIELD_CLASSIFIER));
		
		//topology.newDRPCStream(RPC_CLASSIFIER, drpc).
			//	stateQuery(classifierState, new ClassifierQueryFunction(), new Fields(FIELD_CLASSIFIER));
		
		//topology.newDRPCStream(RPC_EVALUATE, drpc).
				//stateQuery(classifierState,new Fields("args"), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION, FIELD_INSTANCE));
		
		//topology.newDRPCStream(RPC_STATS, drpc).
			//	stateQuery(classifierState,  new StatQueryFunction(), new Fields(FIELD_STATISTICS));
		
		int par_evaluation = 1;
		String evaluation_parallelism = (String)options.get("evaluation.parallelism");
		if (evaluation_parallelism!= null)
			par_evaluation = Integer.parseInt(evaluation_parallelism);
		createPredictionStream(options, topology).parallelismHint(par_evaluation)
			.stateQuery(classifierState,new Fields(FIELD_INSTANCE), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION)).
			each(new Fields(FIELD_PREDICTION, FIELD_INSTANCE), outputQueue(options)); 
		
				
			//	aggregate(new Fields(FIELD_PREDICTION, FIELD_INSTANCE),outputQueue(options),new Fields(FIELD_PREDICTION, FIELD_INSTANCE));
		
		return topology;
	}
	
	


	public abstract Filter outputQueue(Map options);

	public abstract Stream createPredictionStream(Map options, TridentTopology topology);
	public abstract Stream createLearningStream(Map options, TridentTopology topology);

	public abstract LocalDRPC getDRPC(Map options);

	public abstract StateFactory createFactory(String string);
	
	public abstract Classifier getClassifier(Map options);
	
	

}
