/*
 * @(#)XDBConnection.java, 2018年9月21日 下午1:32:05
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver;

import java.io.Closeable;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class XDBConnection implements Closeable
{
    public String host;

    public int port;

    public XDBAccess access;

    public String encoding = XDB.ENCODING_UTF_8;

    XDBConnection(String host, int port, String user, String password)
    {
        this.host = host;
        this.port = port;
        this.access = new XDBAccess(this);

        this.access.login();
    }

    @Override
    public void close()
    {
        this.access.close();
    }

    public int[] putNodes(Node... nodes)
    {
        if (nodes == null || nodes.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }

        return this.access.put(true, nodes);
    }

    public int[] putLinks(Link... links)
    {
        if (links == null || links.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }

        return this.access.put(false, links);
    }

    public Node[] getNodes(int... ids)
    {
        if (ids == null || ids.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }

        return (Node[])this.access.get(true, ids);
    }

    public Link[] getLinks(int... ids)
    {
        if (ids == null || ids.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }

        return (Link[])this.access.get(false, ids);
    }
    
    public boolean[] removeNodes(int... ids)
    {
        if (ids == null || ids.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }
        
        return this.access.remove(true, ids);
    }
    
    public boolean[] removeLinks(int... ids)
    {
        if (ids == null || ids.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }
        
        return this.access.remove(false, ids);
    }
    
    public boolean[] setNodes(Node... nodes)
    {
        if (nodes == null || nodes.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }
        
        return this.access.set(true, nodes);
    }
    
    public boolean[] setLinks(Link... links)
    {
        if (links == null || links.length == 0)
        {
            XDBException.SE_INVALID_PARAM.throwException();
        }
        
        return this.access.set(false, links);
    }
}
