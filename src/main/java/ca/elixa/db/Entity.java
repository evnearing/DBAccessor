package ca.elixa.db;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import io.vertx.core.json.JsonObject;

/**
 * Abstract class, the root of the entity system. Wraps a MongoDB Document with helper methods
 * 
 * All subclasses should implement every constructor.
 * 
 * You do NOT need to provide a key object to this entity; it gets generated from the document.
 * 
 * @author Evan
 *
 */
public abstract class Entity implements Cloneable {
	protected DBService db;
	protected Document raw;
	private Key key;
	private Set<String> projections; //This can be null
	
	private Boolean isNew;

	protected Entity(){};
	
	/**
	 * Internal constructor
	 * @param db
	 * @param raw
	 * @param isNew
	 * @param projections
	 */
	protected void init(DBService db, Document raw, boolean isNew, Set<String> projections) {
		this.db = db;
		this.raw = raw;
		this.isNew = isNew;
		
		this.projections = projections;

		//if the entity is phresh, we need to generate a key for it
		if(isNew) {
			this.key = db.generateKey(getType());
			raw.put("_id", new ObjectId(key.getId()));
		}
		//otherwise, create the key out of the type and ID.
		else {
			this.key = new Key(getType(), raw.getObjectId("_id").toHexString());
		}
	}
	
	/**
	 * TODO consider reflectively grabbing this?
	 */
	public abstract String getType();

	protected abstract <T extends Entity> T instantiate();
	
	public boolean isNew() {
		return isNew;
	}
	
	public boolean projected() {
		return projections != null && false == projections.isEmpty();
	}
	
	/**
	 * This will return null if no projections were set. or at least it should....
	 * @return
	 */
	public Set<String> getProjections(){
		return projections;
	}
	
	public String getName() {
		return raw.getString("name");
	}

	/**
	 * Given a List of Keys on an entity, fetch those entities.
	 * @param key
	 * @return
	 * @param <T>
	 */
	public <T extends Entity> List<T> getReferencedEntityList(String key){
		List<Key> keys = getKeyList(key);

		return db.getEntities(keys);
	}

	/**
	 * Get a list of keys. Can be empty.
	 * @param key
	 * @return
	 */
	public List<Key> getKeyList(String key){
		List<Document> documents = getListValue(key, Document.class);

		if(documents == null || documents.size() == 0){
			return new ArrayList<>();
		}

		List<Key> results = new ArrayList<>();

		for(Document doc : documents)
			results.add(db.getKeyFromDoc(doc));

		return results;
	}

	public void addKeyToList(String targetKey, Entity toAdd){
		addKeyToList(targetKey, toAdd.getKey());
	}

	public void addKeyToList(String targetKey, Key toAdd){
		List<Key> current = getKeyList(targetKey);
		current.add(toAdd);

		setValue(targetKey, current);
	}

	/**
	 * Get a list from this entity
	 * @param key - the given property
	 * @param type - the class of the resulting list type
	 * @return
	 * @param <T> - the type of list
	 */
	protected <T> List<T> getListValue(String key, Class<T> type){
		return raw.getList(key, type);
	}
	
	public Object getValue(String key) {
		return raw.get(key);
	}
	public String getStringValue(String key){
		return (String) getValue(key);
	}
	protected Date getDateValue(String key){
		return raw.getDate(key);
	}

	/**
	 * will return 0L instead of null
	 * @param key
	 * @return
	 */
	protected Long getLongValue(String key){
		Long result = raw.getLong(key);
		if(result == null)
			result = 0L;

		return result;
	}

	/**
	 * will return 0L instead of null
	 * @param key
	 * @return
	 */
	protected Double getDoubleValue(String key){
		Double d = raw.getDouble(key);
		if(d == null)
			return 0d;
		return d;
	}

	protected Boolean getBooleanValue(String key){
		return raw.getBoolean(key);
	}

	/**
	 * Get a string-object map from this entity
	 * @param key - the given property
	 * @return
	 */
	protected Map<String, Object> getMapValue(String key){
		Document doc = getEmbedded(key);

		Map<String, Object> result = new HashMap<>();

		if(doc == null)
			return result;

		for(var entry : doc.entrySet())
			result.put(entry.getKey(), entry.getValue());

		return result;
	}

	protected Map<String, Key> getStringKeyMapValue(String key){
		Map<String, Object> raw = getMapValue(key);

		Map<String, Key> result = new HashMap<>();

		for(var entry : raw.entrySet())
			result.put(entry.getKey(), db.getKeyFromDoc((Document) entry.getValue()));

		return result;
	}

	/**
	 * Get a string-string map from this entity
	 * @param key - the given property
	 * @return
	 */
	protected Map<String, String> getStringStringMapValue(String key){
		Document doc = raw.get(key, Document.class);

		Map<String, String> result = new HashMap<>();

		if(doc == null)
			return result;

		for(var entry : doc.entrySet())
			result.put(entry.getKey(), entry.getValue().toString());

		return result;
	}

	protected Map<String, Double> getStringDoubleMapValue(String key){
		Document doc = raw.get(key, Document.class);

		Map<String, Double> result = new HashMap<>();

		if(doc == null)
			return result;

		for(var entry : doc.entrySet())
			result.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));

		return result;	}

	/**
	 * Fetch a list of embedded entities on this entity.
	 * @param key - the given property
	 * @param type - string representation of the resulting entity type.
	 * @return
	 * @param <T> - the resulting entity type
	 */
	protected <T extends Entity> List<T> getEmbeddedEntityList(String key, String type){
		List<Document> documents = getListValue(key, Document.class);
		List<T> result = new ArrayList<>();

		if(documents == null)
			return result;

		for(Document doc : documents)
			result.add(db.entityService.buildEntity(db, type, doc));
		
		return result;
	}

	/**
	 * Fetch an embedded entity on this entity.
	 * @param key - the given property
	 * @param type - string representation of the resulting entity type.
	 * @return
	 * @param <T> - the resulting entity type
	 */
	protected <T extends Entity> T getEmbeddedEntity(String key, String type){
		return db.entityService.buildEntity(db, type, getEmbedded(key));
	}

	/**
	 * Get an embedded document on this entity.
	 * @param key - the given property
	 * @return
	 */
	private Document getEmbedded(String key){
		return raw.get(key, Document.class);
	}

	/**
	 * Sets a map value onto this entity
	 * @param key - the given property
	 * @param map - the map we are setting
	 */
	protected void setMapValue(String key, Map<String, Object> map){
		Document d = new Document();

		d.putAll(map);

		raw.put(key, d);
	}

	/**
	 *
	 * @param key - the given property
	 * @return an empty array if the value is null
	 */
	protected byte[] getBinaryValue(String key) {
		Object value = getValue(key);
		if(value == null)
			return new byte[0];
		return ((Binary) value).getData();
	}

	/**
	 * Get a Bson Object ID property on this entity
	 * @param key - the given property
	 * @return
	 */
	protected ObjectId getObjectIdValue(String key){
		return (ObjectId) getValue(key);
	}

	/**
	 * Fetches the entity associated with a key value on this entity
	 * @param key - the property we are looking at
	 * @return the entity
	 * @param <T> - the resulting entity type
	 */
	protected <T extends Entity> T getEntityFromKeyValue(String key){
		Key k = getKeyValue(key);

		if(k == null)
			return null;

		return db.getEntity(k);
	}

	/**
	 * Set a value on this entity. It will format the given object accordingly; see @link{BsonService.parseValue}
	 * @param key - the property we are setting
	 * @param value - the new value
	 */
	public void setValue(String key, Object value) {
		raw.put(key, BsonService.parseValue(value));
	}

	/**
	 * Set a binary value onto this entity. Please note that this is required given that byte arrays are primitive.
	 * @param key
	 * @param value
	 */
	protected void setBinaryValue(String key, byte[] value){
		setValue(key, new Binary(value));
	}
	
	protected Key getKeyValue(String property) {
		Document rawKey = (Document) getValue(property);

		if(rawKey == null)
			return null;
		
		return new Key(rawKey);
	}
	
	/**
	 * TODO once the schema is setup, split this into two methods.
	 * One that reads if the entity's schema has a certain field
	 * One that reads if this entity has a non-null value for a field
	 * @param key
	 * @return
	 */
	public boolean hasValue(String key) {
		return raw.containsKey(key);
	}

	/**
	 * @return the key associated with this entity.
	 */
	public Key getKey() {
		return key;
	}

	/**
	 * @return a formatted string representing the key associated with this entity. Same as calling .getKey().toString()
	 */
	public String getKeyString(){
		return getKey().toString();
	}

	/**
	 * @return the ID associated with this entity's key.
	 */
	public String getId() {
		return getKey().getId();
	}

	/**
	 * Do not allow direct access to the raw underlying document.
	 *
	 * INSTEAD, allow them to iterate over an unmodifiable version of the document
	 */
	public void iterateOverAllProperties(Consumer<? super Entry<String, Object>> transformation){
		Collections.unmodifiableCollection(raw.entrySet()).forEach(transformation);
	}

	/**
	 * Creates a perfect copy of this entity, with a unique key.
	 * @return the cloned entity
	 */
	@Override
	public Entity clone(){
		Entity result = db.createEntity(getType());

		iterateOverAllProperties(entry -> {
			//dont copy over the ID
			if(entry.getKey().equals("_id"))
				return;
			result.setValue(entry.getKey(), entry.getValue());
		});

		return result;
	}

	
	/**
	 * Create a Vert.x JsonObject representation of this entity
	 * @return
	 */
	public JsonObject toJson() {
		JsonObject result = new JsonObject();
		
		for(Entry<String, Object> entry : raw.entrySet())
			result.put(entry.getKey(), entry.getValue());
		
		return result;
	}
}
