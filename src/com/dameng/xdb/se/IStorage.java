/*
 * @(#)IStorage.java, 2018年9月10日 下午2:05:11
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se;

import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public interface IStorage
{
    public final static int ID_NULL = 0; // FIXME NSE & RDB
    
    public final static byte FREE_MASK = (byte)0x80;

    public final static byte FREE_TRUE = (byte)0x00;

    public final static byte FREE_FALSE = (byte)0x80;
    
    public final static byte VALUE_TYPE_MASK = 0x0F;

    public void initialize();

    public void destory();

    public int[] putNodes(Node[] nodes);

    public int[] putLinks(Link[] links);

    public Node[] getNodes(int[] ids);

    public Link[] getLinks(int[] ids);

    public boolean[] removeNode(int[] ids);

    public boolean[] removeLink(int[] ids);

    public boolean[] setNode(Node[] nodes);

    public boolean[] setLink(Link[] links);

    public Node[] showNodes(int count);

    public Link[] showLinks(int count);
}
