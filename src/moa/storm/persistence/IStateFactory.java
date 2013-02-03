package moa.storm.persistence;

import java.io.Serializable;

public interface IStateFactory extends Serializable{

	public abstract IPersistentState create();

}