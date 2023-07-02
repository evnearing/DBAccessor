package ca.elixa.db;

import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * This contains an immutable reference to an Entity, by combining its type(collection) and id.
 * @author Evan
 * 
 * TODO consider if we want a method to encode the entire key
 *
 */
public class Key {
	private final String type;
	private final String id;
	protected Key(String type, String id) {
		this.type = type;
		this.id = id;
	}
	protected Key(Document doc) {
		type = doc.getString("type");
		id = doc.getString("id");
	}
	protected Key(String type, ObjectId id) {
		this.type = type;
		this.id = id.toHexString();

	}
	
	/**
	 * Turns this into a document; this is to store it on another document.
	 * @return the composed BSON document
	 */
	public Document toDocument() {
		Document result = new Document();
		
		result.put("type", type);
		result.put("id", id);
		
		return result;
	}
	
	public String getType() {
		return type;
	}
	public String getId() {
		return id;
	}

	@Override
	public String toString(){
		return type + "(" + id + ")";
	}

	/**
	 * Create
	 * @param type
	 * @param id
	 * @return
	 */
	public static Key create(String type, String id){
		return new Key(type, id);
	}
	public static Key create(String string){
		int open = string.indexOf("(");
		int close = string.indexOf(")");

		if(open < 1 || close < 1)
			return null;

		String type = string.substring(0, open);
		String id = string.substring(open + 1, close);

		return create(type, id);
	}
	public static Key create(Document d){
		return new Key(d);
	}

	@Override
	public boolean equals(Object o){
		if(o instanceof Key newKey){

			return newKey.getId().equals(getId()) && newKey.getType().equals(getType());
		}
		return false;
	}
}
