/*
 * @(#)PVStore.java, 2018年10月10日 下午4:30:15
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.nse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * used to store string property value  
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class PVStore
{
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Map<Long, String> map = new HashMap<>();

    private AtomicLong id = new AtomicLong(System.currentTimeMillis());

    public String read(long id)
    {
        try
        {
            lock.readLock().lock();

            return map.get(id);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public long write(String value)
    {
        long id = this.id.getAndIncrement();

        try
        {
            lock.writeLock().lock();

            map.put(id, value);

            return id;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public String free(long id)
    {
        try
        {
            lock.writeLock().lock();

            return map.remove(id);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}
