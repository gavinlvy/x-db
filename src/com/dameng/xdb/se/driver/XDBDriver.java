/*
 * @(#)XDBDriver.java, 2018年9月28日 上午11:00:25
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class XDBDriver
{
    XDBDriver()
    {}
    
    public final static XDBConnection connect(String host, int port, String user, String password)
    {
        return new XDBConnection(host, port, user, password);
    }
}
