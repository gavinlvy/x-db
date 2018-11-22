/*
 * @(#)SEGet.java, 2018年11月8日 下午9:47:13
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.stmt;

import com.dameng.xdb.se.model.GObject;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class SESet extends SEStatement
{
    public GObject<?> obj;
    
    public SESet(boolean node, GObject<?> obj)
    {
        super(TYPE_SE_SET, node);
        this.obj = obj;
    }

    @Override
    public String toString()
    {
        return super.toString() + " - " + obj;
    }
}
