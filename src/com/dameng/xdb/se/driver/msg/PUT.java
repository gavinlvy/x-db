/*
 * @(#)PUT.java, 2018年9月21日 下午2:11:35
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import com.dameng.xdb.se.driver.XDBAccess;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * req: objs
 *      
 * 
 * rsp: id_count(4) + {id}*
 * 
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class PUT extends MSG
{
    public boolean node; // in

    public GObject<?>[] objs; // in

    public int[] ids; // out

    public PUT(Buffer buffer, String encoding)
    {
        super(MSG.COMMAND_PUT, buffer, encoding);
    }

    public static class C extends PUT
    {
        public C(XDBAccess access, boolean node, GObject<?>... objs)
        {
            super(access.buffer, access.connection.encoding);
            this.objs = objs;
            this.node = node;
        }

        @Override
        protected void doEncode()
        {
            // info
            buffer.writeByte(node ? MSG.GOBJ_TYPE_NODE : MSG.GOBJ_TYPE_LINK);

            // objs
            encodeObjects(node, !node, objs);
        }

        @Override
        protected void doDecode()
        {
            decodeException();

            // ids
            ids = decodeIds();
        }
    }

    public static class S extends PUT
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

            // ids
            encodeIds(ids);
        }

        @Override
        protected void doDecode()
        {
            // info
            node = buffer.readByte() == MSG.GOBJ_TYPE_NODE;

            // objs
            objs = decodeObjects(node, !node);
        }
    }
}
