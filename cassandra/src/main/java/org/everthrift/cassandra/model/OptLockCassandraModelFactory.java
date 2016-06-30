package org.everthrift.cassandra.model;

import java.io.Serializable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.CreatedAtIF;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.EntityFactory;
import org.everthrift.appserver.model.EntityNotFoundException;
import org.everthrift.appserver.model.OptResult;
import org.everthrift.appserver.model.OptimisticLockModelFactoryIF;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.appserver.model.UpdatedAtIF;
import org.everthrift.cassandra.DLock;
import org.everthrift.cassandra.SequenceFactory;
import org.everthrift.cassandra.com.datastax.driver.mapping.VersionException;
import org.everthrift.cassandra.com.datastax.driver.mapping.Mapper.Option;
import org.everthrift.thrift.TFunction;
import org.everthrift.utils.LongTimestamp;

import net.sf.ehcache.Cache;

public abstract class OptLockCassandraModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends CassandraModelFactory<PK, ENTITY, E> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>{

    private String sequenceName;
    private SequenceFactory sequenceFactory;

    public OptLockCassandraModelFactory(Cache cache, Class<ENTITY> entityClass) {
        super(cache, entityClass);
    }

    public OptLockCassandraModelFactory(String cacheName, Class<ENTITY> entityClass) {
        super(cacheName, entityClass);
    }

    @Override
    protected void initMapping(){

        super.initMapping();

        if (sequenceName == null){
            sequenceFactory = null;
        }else{
            sequenceFactory = new SequenceFactory(mappingManager.getSession(), "sequences", sequenceName);
        }
    }


    public final void setPkSequenceName(String sequenceName){
        this.sequenceName = sequenceName;

        if (sequenceName == null){
            sequenceFactory = null;
        }else if (mappingManager !=null){
            sequenceFactory = new SequenceFactory(mappingManager.getSession(), "sequences", sequenceName);
        }
    }

    @Override
    public final ENTITY insertEntity(ENTITY e) {
        return optInsert(e).afterUpdate;
    }

    @Override
    public final void deleteEntity(ENTITY e) {
        _delete(e);
    }

    @Override
    public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator) {
        return updateUnchecked(id, mutator, null);
    }

    @Override
    public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, EntityFactory<PK, ENTITY> factory) {
        try {
            return update(id, mutator, factory);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, E {
        return update(id, mutator, null);
    }

    @Override
    public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, EntityFactory<PK, ENTITY> factory) throws TException, E {

        if (id == null)
            throw new IllegalArgumentException("id is null");

        try {
            final OptResult<ENTITY> ret = OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {

                final long now = LongTimestamp.now();

                ENTITY e;

                if (count == 0)
                    e = findEntityById(id);
                else
                    e = fetchEntityById(id);

                final ENTITY orig;

                if (e == null){
                    if (factory == null)
                        throw new EntityNotFoundException(id);

                    e = factory.create(id);

                    if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
                        ((CreatedAtIF)e).setCreatedAt(now);

                    orig = null;
                }else{
                    try {
                        orig = getCopyConstructor().newInstance(e);
                    } catch (Exception e1) {
                        throw new RuntimeException(e1);
                    }
                }

                final boolean needUpdate = mutator.apply(e);
                if (needUpdate == false)
                    return OptResult.create(OptLockCassandraModelFactory.this, e, orig, false);


                //		        if (e instanceof UpdatedAtIF)
                //		        	((UpdatedAtIF) e).setUpdatedAt(now);

                if (orig == null){

                    final DLock lock = this.assertUnique(null, e);
                    final boolean saved;
                    try{
                        saved = mapper.save(e, Option.ifNotExist());
                    }finally{
                        if (lock !=null)
                            lock.unlock();
                    }

                    if (saved){
                        invalidate(id);
                        return OptResult.create(OptLockCassandraModelFactory.this, e, null, true);
                    }else{
                        if (count == 0)
                            invalidate(id);
                        return null;
                    }
                }else{
                    try{

                        final DLock lock = this.assertUnique(orig, e);
                        final boolean updated;
                        try{
                            updated = mapper.update(orig, e, Option.onlyIf());
                        }finally{
                            if (lock !=null)
                                lock.unlock();
                        }

                        if (updated)
                            invalidate(id);

                        return OptResult.create(OptLockCassandraModelFactory.this, e, orig, updated);
                    }catch(VersionException ve){

                        if (count == 0)
                            invalidate(id);

                        return null;
                    }
                }
            });

            if (ret.isUpdated){
                localEventBus.postAsync(updateEntityEvent(ret.beforeUpdate, ret.afterUpdate));
            }

            return ret;
        } catch (EntityNotFoundException e) {
            throw createNotFoundException(id);
        }
    }

    private OptResult<ENTITY> _delete(ENTITY e){
        mapper.delete(e);
        invalidate((PK)e.getPk());
        final OptResult<ENTITY> r= OptResult.create(this, null, e, true);;
        localEventBus.postAsync(deleteEntityEvent(e));
        return r;
    }

    @Override
    public final OptResult<ENTITY> delete(PK id) throws E {

        final ENTITY e = findEntityById(id);

        if (e == null)
            throw createNotFoundException(id);

        return _delete(e);
    }

    @Override
    public final OptResult<ENTITY> optInsert(ENTITY e) {

        final long now = LongTimestamp.now();

        if (e.getPk() == null){
            if (sequenceFactory !=null)
                e.setPk(sequenceFactory.nextId());
            else
                throw new IllegalArgumentException("PK is null:" + e.toString());
        }

        if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
            ((CreatedAtIF)e).setCreatedAt(now);

        if (e instanceof UpdatedAtIF)
            ((UpdatedAtIF) e).setUpdatedAt(now);

        final DLock lock = this.assertUnique(null, e);
        final boolean saved;
        try{
            saved = mapper.save(e, Option.ifNotExist());
        }finally{
            if (lock !=null)
                lock.unlock();
        }

        invalidate((PK)e.getPk());

        if (saved == false)
            throw new UniqueException(null, true, null);

        final OptResult<ENTITY> r = OptResult.create(this, e, null, true);

        localEventBus.postAsync(insertEntityEvent(e));

        return r;
    }

    @Override
    public ENTITY updateEntity(ENTITY e) throws UniqueException {
        throw new NotImplementedException();
    }

    /**
     * Unsafely assume insert without check for update. Use only with uniq (random) PK
     * @param e
     * @return
     */
    public final OptResult<ENTITY> fastInsert(ENTITY e) {
        final long now = LongTimestamp.now();

        if (e instanceof CreatedAtIF && (((CreatedAtIF)e).getCreatedAt() == 0))
            ((CreatedAtIF)e).setCreatedAt(now);

        if (e instanceof UpdatedAtIF)
            ((UpdatedAtIF) e).setUpdatedAt(now);

        putEntity(e, false);
        final OptResult<ENTITY> r = OptResult.create(this, e, null, true);

        localEventBus.postAsync(insertEntityEvent(e));
        return r;
    }
}
