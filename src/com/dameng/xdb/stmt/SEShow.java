/*
 * @(#)SEShow.java, 2018年11月26日 上午11:19:53
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.stmt;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class SEShow extends SEStatement
{
    public int count;

    public SEShow(boolean node, int count)
    {
        super(TYPE_SE_SHOW, node);
        this.count = count;
    }

    @Override
    public String toString()
    {
        return super.toString() + " - " + count;
    }
}
