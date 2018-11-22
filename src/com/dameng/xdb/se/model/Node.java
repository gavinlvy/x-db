/*
 * @(#)Node.java, 2018年9月20日 下午1:59:06
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.model;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Node extends GObject<Node>
{
    public int link;

    public Node(int id)
    {
        super(id);
    }

    public Node(String... categorys)
    {
        super(categorys);
    }
    
    public Node(int id, String... catetorys)
    {
        super(id, catetorys);
    }
    
    @Override
    public String toString()
    {
        return super.toString() + " - (" + link + ")";
    }
}
