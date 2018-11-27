/*
 * @(#)SHOW.java, 2018年11月26日 上午11:23:19
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
public abstract class SHOW extends MSG
{
    public boolean node; // in

    public int count; // in

    public GObject<?>[] objs; // out

    public SHOW(Buffer buffer, String encoding)
    {
        super(MSG.COMMAND_SHOW, buffer, encoding);
    }

    public static class C extends SHOW
    {
        public C(XDBAccess access, boolean node, int count)
        {
            super(access.buffer, access.connection.encoding);
            this.node = node;
            this.count = count;
        }

        @Override
        protected void doEncode()
        {
            // info
            buffer.writeByte(node ? MSG.GOBJ_TYPE_NODE : MSG.GOBJ_TYPE_LINK);

            // count
            buffer.writeInt(count);
        }

        @Override
        protected void doDecode()
        {
            decodeException();

            // objs
            objs = decodeObjects(node, true);
        }
    }

    public static class S extends SHOW
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

            // objs
            encodeObjects(node, true, objs);
        }

        @Override
        protected void doDecode()
        {
            // info
            node = buffer.readByte() == MSG.GOBJ_TYPE_NODE;

            // count
            count = buffer.readInt();
        }
    }
}
