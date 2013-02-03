package moa.storm.persistence;


import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
/** 
 * Interface to the cassandra backend
 * @author bsp
 *
 */
public class CassandraState<T> implements IPersistentState<T>  {


	
    public static class Options<T> implements Serializable {
        public String clusterName = "Test Cluster";
        public int replicationFactor = 1;
        public String keyspace = "test";
        public String columnFamily = "c";
        public String rowKey = "row_key";
    }

    public static class Factory implements IStateFactory  {
        private String hosts;
        private Options options;

        public Factory(String hosts, Options options) {
            this.hosts = hosts;
            this.options = options;
        }
        
        /* (non-Javadoc)
		 * @see moa.storm.persistence.IPenguin#create()
		 */
        @Override
		public IPersistentState create()
        {
        	return new CassandraState(hosts, options);
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
    
    /* (non-Javadoc)
	 * @see moa.storm.persistence.IPersistentState#getLong(java.lang.String, java.lang.String)
	 */
    @Override
	public long getLong(String row, String column) {
		ColumnFamilyResult<String,  String> res = m_template.queryColumns(row);
		Long result = res.getLong(column);
		if (result == null)
			return Long.MIN_VALUE;
		return result.longValue();
    }
    
    /* (non-Javadoc)
	 * @see moa.storm.persistence.IPersistentState#setLong(java.lang.String, java.lang.String, long)
	 */
    @Override
	public void setLong(String row, String column, long value) {
		ColumnFamilyUpdater<String, String> updater = m_template.createUpdater(row);
		updater.setLong(column,value); 
		m_template.update(updater);
    }

	/* (non-Javadoc)
	 * @see moa.storm.persistence.IPersistentState#get(java.lang.String, java.lang.String)
	 */
	@Override
	public T get(String row, String column) {
		ColumnFamilyResult<String,  String> res = m_template.queryColumns(row);
		byte[] data = res.getByteArray(column);
		if (data == null)
			return null;
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(new ByteArrayInputStream(data));
			return (T) is.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);	
		}
		
	}

	/* (non-Javadoc)
	 * @see moa.storm.persistence.IPersistentState#put(java.lang.String, java.lang.String, java.lang.Object)
	 */
	@Override
	public void put(String rowKey, String key, Object value) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os;
			try {
				os = new ObjectOutputStream (bos);
				os.writeObject(value);
				os.flush();
				os.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			ColumnFamilyUpdater<String, String> updater = m_template.createUpdater(rowKey);
			updater.setByteArray(key, bos.toByteArray());
			m_template.update(updater);
	}
	

	
	/* (non-Javadoc)
	 * @see moa.storm.persistence.IPersistentState#deleteRow(java.lang.String)
	 */
	@Override
	public void deleteRow(String rowKey)
	{
		m_template.deleteRow(rowKey);
	}
	
	/* (non-Javadoc)
	 * @see moa.storm.persistence.IPersistentState#deleteColumn(java.lang.String, java.lang.String)
	 */
	@Override
	public void deleteColumn(String rowKey, String columnKey)
	{
		m_template.deleteColumn(rowKey, columnKey);
	}
}