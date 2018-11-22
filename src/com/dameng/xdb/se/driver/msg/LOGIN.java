/*
 * @(#)LOGIN.java, 2018年9月10日 上午10:58:48
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import com.dameng.xdb.XDB;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * 
 * req: empty
 * rsp: encoding
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class LOGIN extends MSG
{
    public LOGIN(Buffer buffer)
    {
        super(MSG.COMMAND_CONNECT, buffer, XDB.ENCODING_UTF_8);
    }

    public static class C extends LOGIN
    {
        public C(Buffer buffer)
        {
            super(buffer);
        }

        @Override
        protected void doEncode()
        {}

        @Override
        protected void doDecode()
        {
            decodeException();
            
            encoding = buffer.readStringWithLength1(encoding);
        }
    }

    public static class S extends LOGIN
    {
        public S(Buffer buffer)
        {
            super(buffer);
        }

        @Override
        protected void doEncode()
        {
            if (encodeException())
            {
                return;
            }
            
            buffer.writeStringWithLength1(encoding, XDB.ENCODING_UTF_8);
        }

        @Override
        protected void doDecode()
        {}
    }
}
