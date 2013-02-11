package performance;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import storm.trident.state.StateFactory;



import moa.classifiers.Classifier;
import moa.core.SizeOf;
import moa.storm.persistence.CassandraState;
import moa.storm.persistence.IStateFactory;
import moa.storm.persistence.IPersistentState;
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
