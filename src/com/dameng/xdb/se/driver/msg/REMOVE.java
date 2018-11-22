/*
 * @(#)REMOVE.java, 2018年11月6日 上午10:11:53
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import com.dameng.xdb.se.driver.XDBAccess;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * req: info(1) + id_count + {id}*
 * 
 * rsp: ret_count + {ret}*
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class REMOVE extends MSG
{
    public boolean node; // in

    public int[] ids; // in

    public boolean[] rets; // out

    public REMOVE(Buffer buffer, String encoding)
    {
        super(MSG.COMMAND_REMOVE, buffer, encoding);
    }

    public static class C extends REMOVE
    {
        public C(XDBAccess access, boolean node, int[] ids)
        {
            super(access.buffer, access.connection.encoding);
            this.node = node;
            this.ids = ids;
        }

        @Override
        protected void doEncode()
        {
            // info
            buffer.writeByte(node ? MSG.GOBJ_TYPE_NODE : MSG.GOBJ_TYPE_LINK);

            // ids
            encodeIds(ids);
        }

        @Override
        protected void doDecode()
        {
            decodeException();

            // rets
            rets = decodeRets();
        }
    }

    public static class S extends REMOVE
    {
        public S(Buffer buffer, String encoding)
        {
            super(buffer, encoding);
        }

        @Override
        protected void doEncode()
        {
            if (encodeException())
            {
                return;
            }

            // rets
            encodeRets(rets);
        }

        @Override
        protected void doDecode()
        {
            // info
            node = buffer.readByte() == MSG.GOBJ_TYPE_NODE;

            // ids
            ids = decodeIds();
        }
    }
}
