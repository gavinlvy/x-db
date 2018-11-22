/*
 * @(#)SEStatement.java, 2018年11月13日 下午3:04:20
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
public class SEStatement extends Statement
{
    public boolean node;
    
    public SEStatement(int type, boolean node)
    {
        super(type);
        this.node = node;
    }

    @Override
    public String toString()
    {
        return node ? "N" : "L";
    }
}
