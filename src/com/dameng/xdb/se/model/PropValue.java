/*
 * @(#)PropValue.java, 2018年10月10日 下午5:16:47
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
public class PropValue
{
    public Object value;

    public byte type = TYPE_STRING;

    public final static byte TYPE_STRING = 0;

    public final static byte TYPE_NUMBERIC = 1;

    public final static byte TYPE_DECIMAL = 2;

    public final static byte TYPE_BOOLEAN = 3;

    public PropValue(Object value, byte type)
    {
        this.value = value;
        this.type = type;
    }

    @Override
    public String toString()
    {
        if (type == TYPE_STRING)
        {
            return value == null ? null : "\'" + value.toString().replaceAll("'", "\\\\'") + "\'";
        }
        else
        {
            return value == null ? null : value.toString();
        }
    }
}
