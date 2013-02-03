package moa.storm.persistence;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSState<T> implements IPersistentState<T>{
	
	private FileSystem m_fs;
	
	public static class Factory implements IStateFactory {
		private HashMap<String,String> m_conf;
		
		public Factory(HashMap<String,String> conf)
		{
			m_conf = conf;
		}

		public IPersistentState create() {
			return new HDFSState(m_conf);
		}
		
	}

	public HDFSState(HashMap<String,String> conf) {
		try {
			Configuration hdfsConf = new Configuration();
			Iterator<String> it = conf.keySet().iterator();
			while (it.hasNext())
			{
				String key = it.next();
				hdfsConf.set(key, conf.get(key));
			}
			m_fs = FileSystem.get(hdfsConf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long getLong(String row, String column) {
		Object val = get(row,column);
		if (val == null)
			return Long.MIN_VALUE;
		return  ((Long)val).longValue();
	}

	private Path createPath(String row, String column) {
		return new Path(row + "/" + column);
	}

	@Override
	public void setLong(String row, String column, long value) {
		put(row,column,value);
	}

	@Override
	public T get(String row, String column) {
		Path p = createPath(row,column);
		FSDataInputStream fis = null;
		ObjectInputStream is = null;
		try{
			fis = m_fs.open(p);
			is = new ObjectInputStream(fis);
			Object o = is.readObject();
			return (T)o;
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		finally {
			try {
				if (is != null ) is.close();
				if (fis != null ) fis.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void put(String rowKey, String key, Object value) {
		Path p = createPath(rowKey, key);
		FSDataOutputStream fos = null;
		ObjectOutputStream os =null;
		try {
			fos = m_fs.create(p);
			os = new ObjectOutputStream(fos);
			os.writeObject(value);
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		finally
		{
			try {
				os.close();
				fos.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void deleteRow(String rowKey) {
		try {
			m_fs.delete(new Path(rowKey), true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteColumn(String rowKey, String columnKey) {
		try {
			m_fs.delete(new Path(rowKey + "/" + columnKey), true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
