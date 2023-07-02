package ca.elixa.db;

import java.util.Set;

import ca.elixa.classpool.ClassPoolString;
import ca.elixa.classpool.GroupedClassPool;
import org.bson.Document;

/**
 * This class is for generating subclasses of {@link Entity}. Implementations of {@link DBService} are responsible
 * for their generation of subclasses.
 * 
 * @author Evan
 *
 */
public class EntityFactory {

	public final ClassPoolString<? extends Entity> pool;

	public EntityFactory(String path){
		pool = new ClassPoolString<>(path, Entity.class);
	}

	public EntityFactory(String... paths){

		GroupedClassPool<Entity> gPool = new GroupedClassPool<>(Entity.class);

		for(String path : paths){
			ClassPoolString<Entity> p = new ClassPoolString<>(path, Entity.class);
			gPool.addPool(p);
		}

		pool = gPool;
	}
	
	protected <T extends Entity> T buildEntity(final DBService db, final String type, Document doc) {
		return createEntityObject(db, type, doc, false, null);
	}

	protected <T extends Entity> T buildEntity(final DBService db, final String type, Document doc, boolean isNew){
		return createEntityObject(db, type, doc, true, null);
	}
	
	protected <T extends Entity> T buildEntity(final DBService db, final String type, Document doc, Set<String> projections) {
		return createEntityObject(db, type, doc, false, projections);
	}

	/**
	 * @param isNew - if this is a fresh entity
	 * @param projections - any projections on this entity.
	 * @param <T extends Entity> - the object type we're creating.
	 * @return the entity.
	 */
	protected <T extends Entity> T createEntityObject(final DBService db, final String type, Document doc, Boolean isNew, Set<String> projections){
		T raw = (T) pool.get(type);
		T result = raw.instantiate();
		result.init(db, doc, isNew, projections);
		return result;
	}
}
