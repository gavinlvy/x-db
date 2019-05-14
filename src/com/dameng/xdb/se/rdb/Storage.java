/*
 * @(#)Storage.java, 2018年12月12日 上午10:33:40
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.rdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.IStorage;
import com.dameng.xdb.se.Processor;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;
import com.dameng.xdb.se.model.PropValue;
import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Storage implements IStorage
{
    private Connection connection;

    private NStore nstore;

    private LStore lstore;

    private PStore pstore;

    private VStore vstore;

    private LTKStore ltkstore;

    @SuppressWarnings ("unused")
    private Processor processor;

    public Storage(Processor processor)
    {
        this.processor = processor;
    }

    @Override
    public void initialize()
    {
        try
        {
            Class.forName(XDB.Config.SE_RDB_DRIVER.value);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("JDBC Driver(" + XDB.Config.SE_RDB_DRIVER + ") load fail!", e);
        }

        try
        {
            this.connection = DriverManager.getConnection(XDB.Config.SE_RDB_URL.value,
                    XDB.Config.SE_RDB_USER.value, XDB.Config.SE_RDB_PASSWORD.value);
        }
        catch (SQLException e)
        {
            throw new RuntimeException("RDB connect fail, database is ready and configures are right?", e);
        }

        try
        {
            nstore = new NStore(this.connection);
            lstore = new LStore(this.connection);
            pstore = new PStore(this.connection);
            vstore = new VStore(this.connection);
            ltkstore = new LTKStore(this.connection);
        }
        catch (SQLException e)
        {
            throw new RuntimeException("RDB store create fail!", e);
        }
    }

    @Override
    public void destory()
    {
        nstore.destory();
        lstore.destory();
        pstore.destory();
        vstore.destory();
        ltkstore.destory();

        MiscUtil.close(this.connection);
    }

    @Override
    public int[] putNodes(Node[] nodes)
    {
        Store.N n = new Store.N();

        int[] rets = new int[nodes.length];

        try
        {
            for (int i = 0; i < nodes.length; ++i)
            {
                rets[i] = nstore.write(n.fill(FREE_FALSE, putGObject(nodes[i]), ID_NULL));
            }
        }
        catch (SQLException e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException();
        }

        return rets;
    }

    @Override
    public int[] putLinks(Link[] links)
    {
        Store.L l = new Store.L();

        int[] rets = new int[links.length];

        try
        {
            for (int i = 0; i < links.length; ++i)
            {
                // TODO check node exists
                rets[i] = lstore.write(l.fill(FREE_FALSE, putGObject(links[i]), links[i].fnode,
                        links[i].tnode, adjustLinkForLinkPut(), ID_NULL, adjustLinkForLinkPut(), ID_NULL));
            }
        }
        catch (SQLException e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException();
        }

        return rets;
    }

    @Override
    public Node[] getNodes(int[] ids)
    {
        Node[] nodes = new Node[ids.length];
        for (int i = 0; i < nodes.length; ++i)
        {

        }

        return nodes;
    }

    @Override
    public Link[] getLinks(int[] ids)
    {
        return null;
    }

    @Override
    public boolean[] removeNode(int[] ids)
    {
        return null;
    }

    @Override
    public boolean[] removeLink(int[] ids)
    {
        return null;
    }

    @Override
    public boolean[] setNode(Node[] nodes)
    {
        return null;
    }

    @Override
    public boolean[] setLink(Link[] links)
    {
        return null;
    }

    @Override
    public Node[] showNodes(int count)
    {
        return null;
    }

    @Override
    public Link[] showLinks(int count)
    {
        return null;
    }

    private int putGObject(GObject<?> obj) throws SQLException
    {
        Store.P p = new Store.P();
        Store.V v = new Store.V();

        // category -> ltk.store & prop.store
        int propId = ID_NULL;
        for (int i = 0; i < obj.categorys.length; ++i)
        {
            propId = pstore.write(p.fill(FREE_FALSE, ID_NULL, ltkstore.put(obj.categorys[i]), propId));
        }

        // properties -> ltk.store & prop.store
        Iterator<Entry<String, PropValue>> iterator = obj.propMap.entrySet().iterator();
        while (iterator.hasNext())
        {
            Entry<String, PropValue> entry = iterator.next();
            String key = entry.getKey();
            PropValue value = entry.getValue();

            // value
            long propValue = 0;
            switch (value.type)
            {
                case PropValue.TYPE_NUMBERIC:
                    propValue = (long)value.value;
                    break;
                case PropValue.TYPE_DECIMAL:
                    propValue = Double.doubleToLongBits((double)value.value);
                    break;
                case PropValue.TYPE_BOOLEAN:
                    propValue = (boolean)value.value ? 1 : 0;
                    break;
                default:
                    propValue = vstore.write(v.fill((String)value.value));
                    break;
            }

            propId = pstore.write(p.fill((byte)(FREE_FALSE | value.type), ltkstore.put(key), propValue,
                    propId));
        }

        return propId;
    }

    private int adjustLinkForLinkPut()
    {
        // TODO
        return 0;
    }

    private int adjustLinkForLinkRemove()
    {
        // TODO
        return 0;
    }
}
