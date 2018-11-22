/*
 * @(#)SEGet.java, 2018年11月8日 下午9:47:13
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.stmt;

import java.util.Arrays;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class SERemove extends SEStatement
{
    public int[] ids;
    
    public SERemove(boolean node, int... ids)
    {
        super(TYPE_SE_REMOVE, node);
        this.ids = ids;
    }

    @Override
    public String toString()
    {
        return super.toString() + " - " + Arrays.toString(ids);
    }
}
