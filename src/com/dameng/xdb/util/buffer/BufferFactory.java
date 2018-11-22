/*
 * @(#)BufferFactory.java, 2018年9月18日 上午10:02:26
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.util.buffer;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class BufferFactory
{
    public final static Buffer wrap(byte[] bytes)
    {
        return Buffer.wrap(bytes);
    }

    public final static Buffer allocate(int capacity)
    {
        return Buffer.allocateBytes(capacity);
    }
}
