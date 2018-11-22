/*
 * @(#)Link.java, 2018年9月20日 下午1:59:06
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
public class Link extends GObject<Link>
{
    public int fnode;

    public int tnode;

    public Link(int id)
    {
        super(id);
    }

    public Link(String... categorys)
    {
        super(categorys);
    }

    public Link(int id, String... categorys)
    {
        super(id, categorys);
    }

    @Override
    public String toString()
    {
        return super.toString() + " - (" + fnode + ", " + tnode + ")";
    }
}
