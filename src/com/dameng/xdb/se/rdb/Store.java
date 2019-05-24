/*
 * @(#)BaseStore.java, 2019年4月14日 下午10:12:17
 *
 * Copyright (c) 2000-2019, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.rdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Store
{
    public final static AtomicLong ID = new AtomicLong(System.currentTimeMillis());

    protected Connection connection;

    protected PreparedStatement getStmt;

    protected PreparedStatement putStmt;

    protected PreparedStatement setStmt;

    protected PreparedStatement removeStmt;

    protected PreparedStatement showStmt;

    public Store(Connection connection)
    {
        this.connection = connection;
    }

    public void initialize() throws Exception
    {
        // create database access statement
    }

    public void destory() throws Exception
    {
        // close database access statement
    }

    public boolean get(Item item) throws Exception
    {
        ResultSet rs = null;
        try
        {
            item.encode(this.getStmt, Item.ENCODE_TYPE_READ);
            rs = this.getStmt.executeQuery();
            if (rs.next())
            {
                item.decode(rs);
                return true;
            }
            return false;
        }
        finally
        {
            MiscUtil.close(rs);
        }
    }

    public int put(Item item) throws Exception
    {
        // TODO
        item.id = (int)ID.getAndIncrement();
        item.encode(this.putStmt, Item.ENCODE_TYPE_WRITE);
        this.putStmt.executeUpdate();
        return item.id;
    }

    public boolean set(Item item) throws Exception
    {
        item.encode(this.setStmt, Item.ENCODE_TYPE_UPDATE);
        return this.setStmt.executeUpdate() > 0;
    }

    public boolean remove(Item item) throws Exception
    {
        item.encode(this.removeStmt, Item.ENCODE_TYPE_REMOVE);
        return this.removeStmt.executeUpdate() > 0;
    }

    /**
     * count fill to item(id)
     */
    @SuppressWarnings ("unchecked")
    public <T extends Item> List<T> show(T item) throws Exception
    {
        List<T> itemList = new ArrayList<>();

        item.encode(this.showStmt, Item.ENCODE_TYPE_SHOW);

        ResultSet rs = null;
        try
        {
            rs = this.showStmt.executeQuery();
            while (rs.next())
            {
                itemList.add((T)item.clone().decode(rs));
            }
        }
        finally
        {
            MiscUtil.close(rs);
        }

        return itemList;
    }

    public abstract static class Item implements Cloneable
    {
        public int id;

        public final static int ENCODE_TYPE_READ = 1;

        public final static int ENCODE_TYPE_WRITE = 2;

        public final static int ENCODE_TYPE_UPDATE = 3;

        public final static int ENCODE_TYPE_REMOVE = 4;

        public final static int ENCODE_TYPE_SHOW = 5;

        public Item fill(int id)
        {
            this.id = id;
            return this;
        }

        @Override
        protected Item clone() throws CloneNotSupportedException
        {
            return (Item)super.clone();
        }

        public abstract Item encode(PreparedStatement pstmt, int type) throws Exception;

        public abstract Item decode(ResultSet rs) throws Exception;
    }
}
