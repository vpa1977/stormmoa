package moa.storm.cassandra;


import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.cassandra.service.spring.HectorTemplateImpl;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
/** 
 * Interface to the cassandra backend
 * @author bsp
 *
 */
public class CassandraState<T>  {

    public static class Options<T> implements Serializable {
        public int localCacheSize = 5000;
        public String globalKey = "$__GLOBAL_KEY__$";
        public Serializer<T> serializer = null;
        public String clusterName = "trident-state";
        public int replicationFactor = 1;
        public String keyspace = "test";
        public String columnFamily = "column_family";
        public String rowKey = "row_key";
    }

    public static class Factory implements StateFactory {
        private String hosts;
        private Options options;

        public Factory(String hosts, Options options) {
            this.hosts = hosts;
            this.options = options;
        }
        
        public CassandraState create()
        {
        	return new CassandraState(hosts, options);
        }

		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			// TODO Auto-generated method stub
			return null;
		}
    }

    private ColumnFamilyTemplate<String,String> m_template;
    private Options<T> options;

    public CassandraState(String hosts, Options<T> options) {
    	Cluster cassandra =HFactory.getOrCreateCluster(options.clusterName, new CassandraHostConfigurator(hosts));
    	Keyspace k_space = HFactory.createKeyspace(options.keyspace, cassandra);
    	m_template = 
    			new ThriftColumnFamilyTemplate<String, String>(k_space,
                        options.columnFamily,
                        StringSerializer.get(),
                        StringSerializer.get());
    	
    	}
    
    public long getLong(String row, String column) {
		ColumnFamilyResult<String,  String> res = m_template.queryColumns(row);
		Long result = res.getLong(column);
		if (result == null)
			return 0;
		return result.longValue();
    }
    
    public void setLong(String row, String column, long value) {
		ColumnFamilyUpdater<String, String> updater = m_template.createUpdater(row);
		updater.setLong(column,value); 
		m_template.update(updater);
    }

	public T get(String row, String column) {
		ColumnFamilyResult<String,  String> res = m_template.queryColumns(row);
		byte[] data = res.getByteArray(column);
		if (data == null)
			return null;
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(new ByteArrayInputStream(data));
			return (T)is.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
			
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public void put(String rowKey, String key, Object value) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(bos);
			os.writeObject(value);
			os.flush();
			os.close();
			ColumnFamilyUpdater<String, String> updater = m_template.createUpdater(rowKey);
			updater.setByteArray(key, bos.toByteArray());
			m_template.update(updater);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void cleanup(String rowKey)
	{
		m_template.deleteRow(rowKey);
	}
}