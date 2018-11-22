/*
 * @(#)SET.java, 2018年11月6日 上午10:52:49
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import com.dameng.xdb.se.driver.XDBAccess;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class SET extends MSG
{
    public boolean node; // in

    public GObject<?>[] objs; // in

    public boolean[] rets; // out

    public SET(Buffer buffer, String encoding)
    {
        super(MSG.COMMAND_SET, buffer, encoding);
    }

    public final static class C extends SET
    {
        public C(XDBAccess access, boolean node, GObject<?>[] objs)
        {
            super(access.buffer, access.connection.encoding);
            this.node = node;
            this.objs = objs;
        }

        @Override
        protected void doEncode()
        {
            // info
            buffer.writeByte(node ? MSG.GOBJ_TYPE_NODE : MSG.GOBJ_TYPE_LINK);

            // objs
            encodeObjects(node, false, objs);
        }

        @Override
        protected void doDecode()
        {
            decodeException();

            // rets
            rets = decodeRets();
        }
    }

    public final static class S extends SET
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

            // objs
            objs = decodeObjects(node, false);
        }
    }
}
