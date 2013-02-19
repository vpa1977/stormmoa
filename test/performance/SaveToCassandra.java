package performance;

import moa.storm.persistence.CassandraState;
import moa.storm.persistence.IPersistentState;
import moa.storm.persistence.IStateFactory;
import moa.streams.generators.RandomTreeGenerator;

public class SaveToCassandra {
	
	public static void main(String[] main) throws Throwable
	{
		
		
		
		CassandraState.Options<String> options = new CassandraState.Options<String>();
		IStateFactory cassandra = new CassandraState.Factory("localhost:9160", options );
		IPersistentState state = cassandra.create();
		
		RandomTreeGenerator gr = new RandomTreeGenerator();
		
		for ( int i = 0; i < 10 ; i ++ )
		{
			long start = System.currentTimeMillis();
			state.put("11s1", "ss11", gr);
			gr = (RandomTreeGenerator)state.get("11s1", "ss11");
			long end = System.currentTimeMillis();
			System.out.println("Done in "+ (end -start));
			
		}


	}
}
