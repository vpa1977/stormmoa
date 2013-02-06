package performance;

import moa.classifiers.Classifier;
import moa.core.DoubleVector;
import moa.options.ClassOption;
import moa.options.Option;
import moa.streams.generators.RandomTreeGenerator;

public class TrainSpeed {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		stream.restart();
		Classifier cls = train(Integer.parseInt(args[0]), stream,0);
		DoubleVector vote = null;
		for (int l =0 ; l < 10; l ++)
		{
			long clock = System.currentTimeMillis();
			for (int i = 0 ;i < 1000; i ++)
			{				
				cls.trainOnInstance(stream.nextInstance());
			}
			long end = System.currentTimeMillis()- clock;
			//System.out.println("100k in "+ (end));
			double oneTuple = (double)end / (double)1000;
			double tuples_sec = 1000/oneTuple;
			System.out.println(tuples_sec);
		}
		System.out.println(vote);
		

	}

	public static Classifier train(int size, RandomTreeGenerator stream,long instances) {
		Classifier cls= null;
		String cliString = "moa.classifiers.meta.OzaBoost -s "+size+" -l \"trees.HoeffdingTree -m 10000000 -e 10000\"";
		try {
			cls = (Classifier)ClassOption.cliStringToObject(cliString, Classifier.class, new Option[]{});
			cls.prepareForUse();
			cls.resetLearning();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		for (int i = 0 ; i < instances; i ++ )
		{
			cls.trainOnInstance(stream.nextInstance());
		}
		return cls;
	}

}
