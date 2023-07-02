package ca.elixa.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This stores information about a query before it is executed. A logical building block in efficient queries
 * @author Evan
 *
 */
public class Query {
	protected Map<String, Pair<FilterOperator, Object>> filters = new HashMap<>();
	protected Map<String, Object> updates = new HashMap<>();
	protected Set<String> projections = new HashSet<>();
	
	private final String type;
	
	
	public Query(String type) {
		this.type = type;
	}
	
	public String getType() {
		return type;
	}
	
	/**
	 * Some queries will modify entities in the DB instead of pulling them
	 * @param propertyName
	 * @param newValue
	 * @return itself
	 */
	public Query addUpdate(String propertyName, Object newValue) {
		
		updates.put(propertyName, newValue);
		
		return this;
	}
	
	/**
	 * Remove a modify query value
	 * @param propertyName
	 * @return itself
	 */
	public Query removeUpdate(String propertyName) {
		updates.remove(propertyName);
		return this;
	}
	
	public Query addProjections(String...properties) {
		for(String s : properties)
			projections.add(s);
		
		return this;
	}
	
	public Query addProjection(String propertyName) {
		projections.add(propertyName);
		
		return this;
	}
	
	public Query removeProjection(String propertyName) {
		projections.remove(propertyName);
		
		return this;
	}

	public Set<String> getProjections(){
		return projections;
	}
	
	/**
	 * 
	 * @param propertyName
	 * @param operator
	 * @param value
	 * @return itself
	 */
	public Query addFilter(String propertyName, FilterOperator operator, Object value) {
		Pair<FilterOperator, Object> duple = new Pair<>(operator, value);
		
		filters.put(propertyName, duple);
		
		return this;
	}
	
	public Query removeFilter(String propertyName) {
		filters.remove(propertyName);
		
		return this;
	}
}
