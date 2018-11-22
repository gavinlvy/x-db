/*
 * @(#)Session.java, 2018年10月29日 下午4:03:08
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se;

import java.util.concurrent.atomic.AtomicLong;

import com.dameng.xdb.XDB;

/**
 * store session information
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Session
{
    public final static AtomicLong COUNTER = new AtomicLong(0);

    public long id = (COUNTER.incrementAndGet() << 48) | System.currentTimeMillis(); // id(8): number(2) + ts(6) 
 
    public String encoding = XDB.Config.ENCODING.value;
    
    public void destory()
    {
        COUNTER.decrementAndGet();
    }

    public static void main(String[] args)
    {
        System.out.println(System.currentTimeMillis());
        System.out.println(new Session().id);
        System.out.println(new Session().id);
    }
}
