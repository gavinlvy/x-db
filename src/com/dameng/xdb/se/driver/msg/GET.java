/*
 * @(#)GET.java, 2018年9月21日 下午2:11:44
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import com.dameng.xdb.se.driver.XDBAccess;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * req: info(1) + id_count(4) + {id}*
 *
 *      info(1)
 *        |- #node or link
 *        
 *        
 * rsp: info(1) + obj_count(4) + {category_count(1) + {category}* + key_value_count(4) + {key + value}*}* + {link | fnode + tnode}*
 * 
 *        info(1)
 *        |- #node or link
 * 
 *        value: val_desc(1) + val_data(x)
 *                |- [xxxx,----] #value type
 *                |- [----,xxxx] #value length
 *                
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class GET extends MSG
{
    public boolean node; // in

    public int[] ids; // in

    public GObject<?>[] objs; // out

    public GET(Buffer buffer, String encoding)
    {
        super(MSG.COMMAND_GET, buffer, encoding);
    }

    public static class C extends GET
    {
        public C(XDBAccess access, boolean node, int... ids)
        {
            super(access.buffer, access.connection.encoding);
            this.ids = ids;
            this.node = node;
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

            // objs
            objs = decodeObjects(node, true);
        }
    }

    public static class S extends GET
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

            // ids
            ids = decodeIds();
        }
    }
}
