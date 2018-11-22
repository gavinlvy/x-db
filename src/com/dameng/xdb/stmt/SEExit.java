/*
 * @(#)SELogin.java, 2018年11月14日 下午5:58:09
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
public class SEExit extends SEStatement
{
    public SEExit()
    {
        super(TYPE_SE_EXIT, false);
    }

    @Override
    public String toString()
    {
        return "EXIT";
    }
}
