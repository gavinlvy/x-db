/*
 * @(#)SEHelp.java, 2018年12月14日 上午9:09:12
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
public class SEHelp extends SEStatement
{
    public SEHelp()
    {
        super(TYPE_SE_HELP, false);
    }

    @Override
    public String toString()
    {
        return "HELP";
    }
}
