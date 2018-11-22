/*
 * @(#)DUMMY.java, 2018年10月28日 上午10:22:52
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import com.dameng.xdb.se.driver.XDBAccess;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class DUMMY extends MSG
{
    public DUMMY(Buffer buffer, String encoding)
    {
        super(MSG.COMMAND_DUMMY, buffer, encoding);
    }

    public final static class C extends DUMMY
    {
        public C(XDBAccess access)
        {
            super(access.buffer, access.connection.encoding);
        }

        @Override
        protected void doEncode()
        {}

        @Override
        protected void doDecode()
        {}
    }

    public final static class S extends DUMMY
    {
        public S(Buffer buffer, String encoding)
        {
            super(buffer, encoding);
        }

        @Override
        protected void doEncode()
        {}

        @Override
        protected void doDecode()
        {}
    }
}
