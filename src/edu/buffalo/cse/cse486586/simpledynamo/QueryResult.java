package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class QueryResult implements Serializable{
	private static final long serialVersionUID = 1L;
	Map<String,String> results;
	
	public QueryResult()
	{
		results = new HashMap<String,String>();
	}
	
	public QueryResult(Map<String,String> results) {
		this.results = results;
	}
	
	public Map<String,String> getResults() {
		return results;
	}
	
	public void setResults(Map<String,String> results) {
		this.results = results;
	}
		
	public String toString()
	{
		return(results.toString());
	}
	
	
	
}
