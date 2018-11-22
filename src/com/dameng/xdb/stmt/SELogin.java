/*
 * @(#)SELogin.java, 2018年11月14日 下午5:58:09
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.stmt;

import com.dameng.xdb.util.StringUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class SELogin extends SEStatement
{
    public String host;
    
    public int port;
    
    public String user;
    
    public String password;
    
    public SELogin(String host, int port)
    {
        super(TYPE_SE_LOGIN, false);
        this.host = host;
        this.port = port;
        this.user = StringUtil.EMPTY;
        this.password = StringUtil.EMPTY;
    }
    
    public SELogin(String host, int port, String user, String password)
    {
        super(TYPE_SE_LOGIN, false);
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString()
    {
        return host + ":" + port + "@" + user + "/" + password;
    }
}
