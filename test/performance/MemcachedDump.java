package performance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;

public class MemcachedDump {
	public static void main(String[] args) throws IOException
	{
		String key = args[0];
		final Serializer serializer = new JSONTransactionalSerializer();
	     ConnectionFactoryBuilder builder =
                 new ConnectionFactoryBuilder()
                     .setTranscoder(new Transcoder<Object>() {

            
             public boolean asyncDecode(CachedData cd) {
                 return false;
             }

             
             public CachedData encode(Object t) {
                 return new CachedData(0, serializer.serialize(t), CachedData.MAX_SIZE);
             }

           
             public Object decode(CachedData data) {
            	 if (data.getData() == null)
            	 {
            		 System.out.println("Not Found");
            		 return null;
            	 }	 
            	 System.out.println(data.getData());
            	 
                 return serializer.deserialize(data.getData());
             }

           
             public int getMaxSize() {
                 return CachedData.MAX_SIZE;
             }
         });
	     
	     List<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
	     servers.add( new InetSocketAddress("localhost", 10001));
	     MemcachedClient client = new MemcachedClient(builder.build(), servers);
	     for (int i = 0 ;i < 33 ; i ++)
	    	 System.out.println(client.get(key+i));
	}
}
