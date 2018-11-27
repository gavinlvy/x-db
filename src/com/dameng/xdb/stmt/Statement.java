/*
 * @(#)BaseStatement.java, 2018年5月8日 上午10:53:36
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
public class Statement
{
    // SE
    
    public final static int TYPE_SE_EXIT = 100;
    
    public final static int TYPE_SE_LOGIN = 101;
    
    public final static int TYPE_SE_LOGOUT = 102;
    
    public final static int TYPE_SE_PUT = 103;

    public final static int TYPE_SE_GET = 104;

    public final static int TYPE_SE_SET = 105;

    public final static int TYPE_SE_REMOVE = 106;
    
    public final static int TYPE_SE_SHOW = 107;

    public int type;

    public Statement(int type)
    {
        this.type = type;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
