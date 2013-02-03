package performance.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.DeflaterOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import performance.FastRun;



import moa.classifiers.Classifier;
import moa.core.SizeOf;
import moa.storm.persistence.CassandraState;
import moa.streams.generators.RandomTreeGenerator;

public class SaveToHadoop {
	
	public static void main(String[] main) throws Throwable
	{
		RandomTreeGenerator stream = new RandomTreeGenerator();
		stream.prepareForUse();
		stream.restart();
		Classifier mightyClassifer = FastRun.train(100, stream, 100000);
		long magic_size = SizeOf.fullSizeOf(mightyClassifer);
		try {
			ObjectOutputStream os = new ObjectOutputStream(new DeflaterOutputStream(new FileOutputStream(new File("/home/bsp/smallensemble.object"))));
			os.writeObject(mightyClassifer);
			os.flush();
			os.close();
		}
		catch (Throwable t){
			t.printStackTrace();
		}
		
		
		String file = "/home/bsp/smallensemble.object";
		ObjectInputStream is =  new ObjectInputStream(new FileInputStream(file));
		Object o = is.readObject();
		
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://localhost:9000");
		config.set("dfs.replication", "1");
		
				
		FileSystem fs  = FileSystem.get(config);
		fs.mkdirs(new Path("key/value"));
		
		for ( int i = 0; i < 10 ; i ++ )
		{
			long start = System.currentTimeMillis();
			fs.delete(new Path("key/value/bigensemble1.object"));
			FSDataOutputStream out = fs.create(new Path("key/value/bigensemble1.object"));
			ObjectOutputStream os = new ObjectOutputStream(out);
			os.writeObject(o);
			
			out.flush();
			out.close();
			long end = System.currentTimeMillis();
			System.out.println("Done in "+ (end -start));
			
		}


	}
}
