/*
 * @(#)VStore.java, 2018年10月10日 下午7:30:15
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
 * Property Value storage  
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class VStore extends Store
{
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Map<Long, String> map = new HashMap<>();

    private AtomicLong id = new AtomicLong(System.currentTimeMillis());

    public String get(long id)
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
    
    public long put(String value)
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

    public String remove(long id)
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
