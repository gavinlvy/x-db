/*
 * @(#)MiscUtil.java, 2018年10月28日 上午10:35:10
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.util;

import java.io.Closeable;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class MiscUtil
{
    public static void close(Closeable obj)
    {
        try
        {
            obj.close();
        }
        catch (Exception e)
        {}
    }
    
    public static void close(AutoCloseable obj)
    {
        try
        {
            obj.close();
        }
        catch (Exception e)
        {}
    }
}
