/*
 * @(#)LTKStore.java, 2018年9月14日 下午5:06:09
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.nse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dameng.xdb.util.StringUtil;

/**
 * used to store: node_label/link_type/property_key
 * 
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class LTKStore
{
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Map<String, Integer> map = new HashMap<>();

    private List<String> array = new ArrayList<String>();

    public LTKStore()
    {
        array.add(StringUtil.EMPTY); // id 0 is reserved, used as id null.
    }
    
    public String read(int id)
    {
        try
        {
            lock.readLock().lock();

            if (id < 0 || id >= array.size())
            {
                return null;
            }
            return array.get(id);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public int write(String value)
    {
        try
        {
            lock.writeLock().lock();

            Integer id = map.get(value);
            if (id == null)
            {
                id = array.size();
                map.put(value, id);
                array.add(value);
            }
            return id;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}
