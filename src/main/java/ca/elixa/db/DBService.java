package ca.elixa.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ca.elixa.iris.Iris;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.client.model.Filters;


/**
 * The main point of access to MongoDB
 * My goal is to not expose the MongoDB api outside of this package.
 *
 * See {@link EntityFactory} for the generation of entity objects.
 * 
 * @author Evan
 *
 * TODO consider caching the collections we've called?
 *
 * TODO make this abstract. Remove entityfactory and implement its functionality in here as an abstract method.
 *
 */
public class DBService{
	protected final EntityFactory entityService;
	private final MongoClient client;
	protected final MongoDatabase db;
	protected final ClientSession session;

	public DBService(MongoClient client, EntityFactory cs) {
		entityService = cs;
		this.client = client;
		session = client.startSession();
		db = client.getDatabase(getDBName());

	}

	//TODO this is probably an environment variable
	protected String getDBName() {
		return "gfl-test";
	}

	/**
	 * Perform an action inside of a mongodb transaction.
	 * This isn't ThreadSafe
	 *
	 * @param action - what is being run inside the script context
	 * @return
	 */
	public boolean doTransaction(Runnable action) {

		action.run();

		return true;

		//try{
		//	return true;
		//}
		//catch(Exception e){
		//	e.printStackTrace();
		//	return false;
		//}

		/*
        //TODO consider transactionOptions
        return session.withTransaction(() -> {
            try {
                action.run();
                return true;
            }
            //if any error occurs, we simply report back. Might be worth throwing E instead? TODO
            catch(Exception e) {
                return false;
            }
        });

		 */
	}

	public Long test(){
		Bson filter = BsonService.getFilterForId("621b087015fbea9eed172e7c");

		Document result = fetchRawInternal("test", filter, new Document()).get(0);

		return result.getLong("Visits");
	}

	public void testIncrement(){

		Boolean test = doTransaction(() -> {
			Bson filter = BsonService.getFilterForId("621b087015fbea9eed172e7c");

			Document result = fetchRawInternal("test", filter, new Document()).get(0);

			Long value = result.getLong("Visits");
			value = value+1;
			result.put("Visits", value);
			result.put("NewValue", value);

			System.out.println(value);

			MongoCollection<Document> col = db.getCollection("test");
			col.replaceOne(filter, result);

		});

		if(!test) System.out.println("a whoopsie has happened");
	}

	//use key.create
	@Deprecated
	public Key getKeyFromString(String raw){
		return Key.create(raw);
	}

	@Deprecated
	public Key getKeyFromDoc(Document doc){
		if(doc == null)
			return null;
		return new Key(doc);
	}

	@Deprecated
	public Key getKey(String type, String id) {
		return new Key(type, id);
	}

	public <T extends Entity> T createEntity(String type){
		return entityService.buildEntity(this, type, new Document(), true);
	}

	public Key generateKey(String type) {
		
		boolean resolved = false;
		
		ObjectId random = null;
		
		while(!resolved) {
			random = new ObjectId();
			
			Key key = new Key(type, random.toHexString());

			if(entityExists(key))
				resolved = true;
		}
		
		return new Key(type, random.toHexString());
	}
	

	
	/**
	 * Inserts a single entity into the DB
	 * @param ent
	 */
	public void put(Entity ent) {
		MongoCollection<Document> col = db.getCollection(ent.getType());
		
		putInternal(ent, col);
	}
	
	/**
	 * Same as calling put(Iterable<Entity>)
	 * @param entities
	 */
	public void put(Entity...entities ) {
		put(Arrays.asList(entities));
	}
	
	/**
	 * Insert a list of entities into the database
	 * @param ents
	 */
	public <T extends Entity> void put(Iterable<T> ents) {
		Map<String, List<T>> sorted = sortEntitiesByType(ents);
		
		for(Entry<String, List<T>> entry : sorted.entrySet()) {
			MongoCollection<Document> col = db.getCollection(entry.getKey());
						
			for(Entity ent : entry.getValue())
				putInternal(ent, col);
		}
	}

	private boolean bulkPutMode = false;
	private List<Entity> pending = null;
	public void startBulkPutMode(){
		bulkPutMode = true;
	}

	public void bulkCommit(){
		bulkPutMode = false;
		put(pending);
		pending = null;
	}
	
	/**
	 * Put an entity to the DB.
	 * @param ent - the Entity to put
	 * @param col - the MongoCollection we're putting this document to
	 */
	private void putInternal(Entity ent, MongoCollection<Document> col) {

		if(bulkPutMode) {
			if (pending == null)
				pending = new ArrayList<>();

			pending.add(ent);

			return;
		}

		
		//TODO consider a put event handler
		
		if(ent.projected()) {
			Iris.warning("Attempted to save projected entity of key");
			return;
		}

		Iris.debug("SAVING ENTITY " + ent.getId() + " of type " + ent.getType() + " of name " + ent.getName());


		if(ent.isNew())
			col.insertOne(session, ent.raw);
		else
			col.replaceOne(session, BsonService.getFilterForId(ent.getId()), ent.raw);
	}
	
	public void deleteEntity(Entity ent) {		
		delete(ent.getKey());
	}
	
	/**
	 * Delete entities from the DB. This is the same as deleteEntity(Iterable)
	 * @param entities
	 */
	public void deleteEntity(Entity...entities) {
		deleteEntity(Arrays.asList(entities));
	}
	
	/**
	 * Delete entities from the DB.
	 * @param entities
	 */
	public void deleteEntity(Iterable<Entity> entities) {
		List<Key> keys = getKeysFromEntities(entities);
		
		delete(keys);
	}
	
	/**
	 * Delete an entity from the DB based on its key
	 * @param key
	 */
	public void delete(Key key) {
		MongoCollection<Document> col = db.getCollection(key.getType());
		
		deleteInternal(key, col);
	}
	/**
	 * Delete a collection of entities by their key. This is the same as delete(Iterable)
	 * @param keys
	 */
	public void delete(Key...keys) {
		delete(Arrays.asList(keys));
	}
	/**
	 * Delete a collection of entities by their key.
	 * @param keys
	 */
	public void delete(Iterable<Key> keys) {
		
		Map<String, List<Key>> sorted = sortKeysByType(keys);
		
		for(Entry<String, List<Key>> entry : sorted.entrySet()) {
			MongoCollection<Document> col = db.getCollection(entry.getKey());
			
			for(Key key : entry.getValue())
				deleteInternal(key, col);
		}
	}
	
	/**
	 * Delete a key from the DB
	 * @param key
	 * @param col
	 */
	private void deleteInternal(Key key, MongoCollection<Document> col) {
		col.deleteOne(session, BsonService.getFilterForId(key.getId()));
	}
	
	/**
	 * Fetch a list of entities from a list of keys.
	 * 
	 * All of these entities will not *necessarily* be the same type. You will need to cast to the appropriate type.
	 * @param keys
	 * @return
	 */
	public <T extends Entity> List<T> getEntities(Iterable<Key> keys){
		List<T> result = new ArrayList<>();

		Map<String, List<Key>> sorted = sortKeysByType(keys);

		for(Entry<String, List<Key>> entry : sorted.entrySet()) {
			String type = entry.getKey();
			List<ObjectId> ids = new ArrayList<>();

			for(Key key : entry.getValue()) 
				ids.add(new ObjectId(key.getId()));
			
			Bson filter = Filters.in("_id", ids);
			
			MongoCollection<Document> col = db.getCollection(type);
			
			for(Document doc : col.find(session, filter)) {
				result.add(entityService.buildEntity(this, type, doc));
			}
		}
		
		return result;
	}

	public <T extends Entity> T getEntity(String type, String id){
		return getEntity(new Key(type, id));
	}
	
	/**
	 * Fetch a single entity from a key.
	 * @param key
	 * @return
	 */
	public <T extends Entity> T getEntity(Key key) {
		
		List<Document> docs = fetchRawInternal(key.getType(), BsonService.getFilterForId(key.getId()), null);

		if(docs.size() == 0)
			return null;
		
		if(docs.size() != 1)
			throw new IllegalStateException("cant have multiple docs with the same identifier. delete this project.");
		
		return entityService.buildEntity(this, key.getType(), docs.get(0));
	}

	/**
	 * @param key
	 * @return true if an entity already exists for the given key.
	 */
	public boolean entityExists(Key key){
		MongoCollection<Document> collection = db.getCollection(key.getType());
		return collection.countDocuments(session, BsonService.getFilterForId(key.getId())) == 0;
	}
	
	/**
	 * returns a list of built entities, given a type and a bson filter.
	 * @param type
	 * @param filter
	 * @return
	 */
	protected <T extends Entity> List<T> fetchInternal(String type, Bson filter){
		return fetchInternal(type, filter, null);
	}
	
	/**
	 * Returns a list of built entities, given a type and a Bson Filter. Any group fetch runs through this method.
	 * We create the entities using this.
	 * @param type - the entity type we're fetching
	 * @param filter - the composed bson filter
	 * @param projections - a set of the fields we're projection. This can be null.
	 * @return
	 */
	protected <T extends Entity> List<T> fetchInternal(String type, Bson filter, Set<String> projections){
		
		Bson composedProj = BsonService.generateProjections(projections);
		
		List<Document> rawList = fetchRawInternal(type, filter, composedProj);
		List<T> result = new ArrayList<>();
		
		for(Document doc : rawList) {
			result.add(entityService.buildEntity(this, type, doc));
		}
		
		return result;
	}



	private List<Document> fetchRawInternal(String collection, Bson filter, Bson projections){
		return fetchRawInternal(collection, filter, projections, Integer.MAX_VALUE); //this might be unwise
	}
	
	/**
	 * Fetches a raw list of documents from the given collection.
	 * @param collection - the type of entity
	 * @param filter - the composed Bson filters
	 * @param projections - the composed Bson projections
	 * @return
	 */
	private List<Document> fetchRawInternal(String collection, Bson filter, Bson projections, int limit) {
		List<Document> result = new ArrayList<>();

		MongoCollection<Document> col = db.getCollection(collection);

		//I think this is better???
		try(MongoCursor<Document> test = col.find(filter).projection(projections).limit(limit).iterator()){
			while(test.hasNext()){
				result.add(test.next());
			}
		}

		return result;
	}

	/**
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 * queries
	 */
	public <T extends Entity> List<T> runEntityQuery(Query q){
		Bson filter = BsonService.generateCompositeFilter(q.filters);

		return fetchInternal(q.getType(), filter, q.projections);
	}

	public void runDeleteQuery(Query q) {
		Bson filter = BsonService.generateCompositeFilter(q.filters);

		db.getCollection(q.getType()).deleteMany(session, filter);

	}
	public void runUpdate(Query q) {
		Bson filters = BsonService.generateCompositeFilter(q.filters);
		Bson updates = BsonService.generateUpdates(q.updates);

		db.getCollection(q.getType()).updateMany(session, filters, updates);
	}
	public Long runCount(Query q) {
		Bson filters = BsonService.generateCompositeFilter(q.filters);

		return db.getCollection(q.getType()).countDocuments(session, filters);
	}


	
	/**
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	 *
	Below this point are helper methods for sorting entities and extracting information from a collection of entities/keys
	 */
	
	/**
	 * Extracts a list of keys from a list of entities
	 * @param entities
	 * @return
	 */
	public List<Key> getKeysFromEntities(Iterable<Entity> entities){
		List<Key> result = new ArrayList<>();
		
		for(Entity ent : entities)
			result.add(ent.getKey());
		
		return result;
	}
	
	/**
	 * Extracts a list of documents from a list of entities
	 * @param entities
	 * @return
	 */
	protected List<Document> getRawEntities(Iterable<Entity> entities){
		List<Document> result = new ArrayList<>();
		
		for(Entity ent : entities) 
			result.add(ent.raw);
		
		return result;
	}
	
	/**
	 * Sorts a list of entities into a map of string->list->entity
	 * TODO consider exposing this method (if needed)
	 * @param ents
	 * @return
	 */
	private <T extends Entity> Map<String, List<T>> sortEntitiesByType(Iterable<T> ents){
		Map<String, List<T>> result = new HashMap<>();
		
		for(T ent : ents) {
			String type = ent.getType();
			
			List<T> current = result.get(type);
			if(current == null) current = new ArrayList<>();
			
			current.add(ent);
			
			result.put(type, current);
		}
		
		return result;
	}
	
	/**
	 * Sorts a list of keys into a map of type-list<key>
	 * @param keys
	 * @return
	 */
	private Map<String, List<Key>> sortKeysByType(Iterable<Key> keys){
		Map<String, List<Key>> result = new HashMap<>();
		
		for(Key key : keys) {
			if(key == null)
				continue;
			String type = key.getType();
			
			List<Key> current = result.get(type);
			if(current == null) current = new ArrayList<>();
			
			current.add(key);
			
			result.put(type, current);
		}
		
		return result;
	}
}
