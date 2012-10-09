package moa.storm.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

import moa.classifiers.Classifier;
import moa.core.MiscUtils;
import moa.storm.tasks.ClassifierQueryFunction;
import moa.storm.tasks.EvaluateQueryFunction;
import moa.storm.tasks.LearnerAggregator;
import moa.storm.tasks.LearnerWrapper;
import moa.storm.tasks.StatQueryFunction;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.Filter;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.StateFactory;
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
	private static final String FIELD_PREDICTION = "prediction";
	private static final String FIELD_INSTANCE = "instance";
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
		StateFactory factory = createFactory(options); 
		LearnerWrapper wrapper = new LearnerWrapper( getClassifier(options) );
		
		int ensemble_size = Integer.parseInt(String.valueOf(options.get(BAGGING_ENSEMBLE_SIZE)));
		TridentState[] classifierState = new TridentState[ensemble_size];
		
		Stream learningStream = createLearningStream(options, topology);
		for (int i = 0; i < ensemble_size; i++)
		{
			classifierState[i] = learningStream.each(new Fields(FIELD_INSTANCE), new Deserialize(),new Fields(FIELD_WEIGHT) ).
				each(new Fields(FIELD_INSTANCE, FIELD_WEIGHT), new BaseFilter(){
					@Override
					public boolean isKeep(TridentTuple tuple) {
						Integer field = tuple.getIntegerByField(FIELD_WEIGHT);
						if (field > 0) {
							Instance instance = (Instance)tuple.getValueByField(FIELD_INSTANCE);
							instance.setWeight(instance.weight() * field);
							return true;
						}
						return false;
					}
				}).		
					persistentAggregate(factory, new Fields(FIELD_INSTANCE), 
							new LearnerAggregator(wrapper), new Fields(FIELD_CLASSIFIER));	
		}
		TridentState readCount = learningStream.persistentAggregate(factory, new ReducerAggregator<Integer>(){

			@Override
			public Integer init() {
				// TODO Auto-generated method stub
				return new Integer(0);
			}

			@Override
			public Integer reduce(Integer curr, TridentTuple tuple) {
				if (curr == null)
					curr = init();
				return curr+1;
			}
			
		}, new Fields("count_instances"));
				
		ArrayList<Stream> merge = new ArrayList<Stream>();
		Stream evaluate = topology.newDRPCStream(RPC_EVALUATE, drpc);
		for (int i = 0; i < ensemble_size; i++)
		{
			merge.add(evaluate.
					stateQuery(classifierState[i],new Fields("args"), 
								new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION, FIELD_INSTANCE)));
		}
		topology.merge(merge).groupBy(new Fields(FIELD_INSTANCE)).aggregate(new BaggingAggregator(),  new Fields(FIELD_PREDICTION));
				
		
		topology.newDRPCStream(RPC_STATS, drpc).
				stateQuery(readCount,  new BaseQueryFunction<ReadOnlySnapshottable<Integer>, Integer>(){

					@Override
					public List<Integer> batchRetrieve(
							ReadOnlySnapshottable<Integer> state,
							List<TridentTuple> args) {
						ArrayList<Integer> res = new ArrayList<Integer>();
						res.add( state.get());
						return res;
					}

					@Override
					public void execute(TridentTuple tuple, Integer result,
							TridentCollector collector) {
						ArrayList<Object> list = new ArrayList<Object>();
						if (result != null)
							list.add(result);
						else
							list.add( new Long(0));
						collector.emit(list);
						
					}
					
				}, new Fields(FIELD_STATISTICS));
				
		
		
		/*createEvaluationStream(options, topology). 
			stateQuery(classifierState,new Fields("args"), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION, FIELD_INSTANCE)).
				each(new Fields(FIELD_PREDICTION, FIELD_INSTANCE), outputQueue(options));*/
		
		return topology;

	}

	public TridentTopology create(Map options)
	{
		TridentTopology topology = new TridentTopology();
		
		LocalDRPC drpc = getDRPC(options);
		StateFactory factory = createFactory(options); 
		LearnerWrapper wrapper = new LearnerWrapper( getClassifier(options) );
		
		Stream learningStream = createLearningStream(options, topology);
		TridentState classifierState = learningStream.persistentAggregate(factory, new Fields(FIELD_INSTANCE), 
				new LearnerAggregator(wrapper), new Fields(FIELD_CLASSIFIER));
		
		topology.newDRPCStream(RPC_CLASSIFIER, drpc).
				stateQuery(classifierState, new ClassifierQueryFunction(), new Fields(FIELD_CLASSIFIER));
		
		topology.newDRPCStream(RPC_EVALUATE, drpc).
				stateQuery(classifierState,new Fields("args"), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION, FIELD_INSTANCE));
		
		topology.newDRPCStream(RPC_STATS, drpc).
				stateQuery(classifierState,  new StatQueryFunction(), new Fields(FIELD_STATISTICS));
		
		createPredictionStream(options, topology)
			.stateQuery(classifierState,new Fields(FIELD_INSTANCE), new EvaluateQueryFunction(), new Fields(FIELD_PREDICTION)).
				each(new Fields(FIELD_PREDICTION, FIELD_INSTANCE), outputQueue(options));
		
		return topology;
	}

	public abstract Filter outputQueue(Map options);

	public abstract Stream createPredictionStream(Map options, TridentTopology topology);
	public abstract Stream createLearningStream(Map options, TridentTopology topology);

	public abstract LocalDRPC getDRPC(Map options);

	public abstract StateFactory createFactory(Map options);
	
	public abstract Classifier getClassifier(Map options);
	
	

}
