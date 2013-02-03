package moa.storm.persistence;

import java.io.Serializable;




public interface IPersistentState<T> extends Serializable {

	public abstract long getLong(String row, String column);

	public abstract void setLong(String row, String column, long value);

	public abstract T get(String row, String column);

	public abstract void put(String rowKey, String key, Object value);

	public abstract void deleteRow(String rowKey);

	public abstract void deleteColumn(String rowKey, String columnKey);

}