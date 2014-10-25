package edu.buffalo.cse.cse486586.simpledynamo;

public class DynamoNode {
	private String node_id;
	private String node_port;
	private String node_avd;
	private String previous_node_id;
	private String previous_node_port;
	private String previous_node_avd;
	private String next_node_id;
	private String next_node_port;
	private String next_node_avd;
	
	public DynamoNode()
	{
		this.node_id = null;
		this.node_port = null;
		this.node_avd = null;
	}
	
	public DynamoNode(String node_id, String node_port, String node_avd) {
		this.node_id = node_id;
		this.node_port = node_port;
		this.node_avd = node_avd;
	}
	
	public String getNode_id() {
		return node_id;
	}
	
	public void setNode_id(String node_id) {
		this.node_id = node_id;
	}
	
	public String getNode_port() {
		return node_port;
	}
	
	public void setNode_port(String node_port) {
		this.node_port = node_port;
	}
	
	public String getNode_avd() {
		return node_avd;
	}
	
	public void setNode_avd(String node_avd) {
		this.node_avd = node_avd;
	}

	public String getPrevious_node_id() {
		return previous_node_id;
	}

	public void setPrevious_node_id(String previous_node_id) {
		this.previous_node_id = previous_node_id;
	}

	public String getPrevious_node_port() {
		return previous_node_port;
	}

	public void setPrevious_node_port(String previous_node_port) {
		this.previous_node_port = previous_node_port;
	}

	public String getPrevious_node_avd() {
		return previous_node_avd;
	}

	public void setPrevious_node_avd(String previous_node_avd) {
		this.previous_node_avd = previous_node_avd;
	}

	public String getNext_node_id() {
		return next_node_id;
	}

	public void setNext_node_id(String next_node_id) {
		this.next_node_id = next_node_id;
	}

	public String getNext_node_port() {
		return next_node_port;
	}

	public void setNext_node_port(String next_node_port) {
		this.next_node_port = next_node_port;
	}

	public String getNext_node_avd() {
		return next_node_avd;
	}

	public void setNext_node_avd(String next_node_avd) {
		this.next_node_avd = next_node_avd;
	}
	
	@Override
	public String toString()
	{
		return("Prev="+this.previous_node_port+"Me="+this.node_port+"Next="+this.next_node_port);
	}

}
