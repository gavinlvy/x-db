/*
 * @(#)Store.java, 2019年5月14日 下午8:11:28
 *
 * Copyright (c) 2000-2019, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.nse;

import com.dameng.xdb.XDB;
import com.dameng.xdb.se.IStorage;
import com.dameng.xdb.util.ByteUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Store
{
    public static abstract class Item
    {
        public byte info;

        public Item()
        {}

        public void free()
        {
            info = (byte)(info & ~IStorage.FREE_MASK | IStorage.FREE_TRUE);
        }

        public boolean isFree()
        {
            return (info & IStorage.FREE_MASK) == IStorage.FREE_TRUE;
        }

        public abstract int length();

        public abstract void encode(byte[] bytes, int offset);

        public abstract void decode(byte[] bytes, int offset);
    }

    /**
     * NODE: info(1) + prop(4) + link(4) #9Bytes
     * 
     * @author ychao
     * @version $Revision: $, $Author: $, $Date: $
     */
    public static class N extends Item
    {
        public final static int LENGTH = 9;

        public final static int OFFSET_INFO = 0;

        public final static int OFFSET_PROP = OFFSET_INFO + XDB.BYTE_SIZE;

        public final static int OFFSET_LINK = OFFSET_PROP + XDB.INT_SIZE;

        public int prop;

        public int link;

        public N fill(byte info, int prop, int link)
        {
            this.info = info;
            this.prop = prop;
            this.link = link;
            return this;
        }

        @Override
        public int length()
        {
            return LENGTH;
        }

        @Override
        public void encode(byte[] bytes, int offset)
        {
            ByteUtil.setByte(bytes, offset + OFFSET_INFO, info);
            ByteUtil.setInt(bytes, offset + OFFSET_PROP, prop);
            ByteUtil.setInt(bytes, offset + OFFSET_LINK, link);
        }

        @Override
        public void decode(byte[] bytes, int offset)
        {
            info = ByteUtil.getByte(bytes, offset + OFFSET_INFO);
            prop = ByteUtil.getInt(bytes, offset + OFFSET_PROP);
            link = ByteUtil.getInt(bytes, offset + OFFSET_LINK);
        }

        @Override
        public String toString()
        {
            return "info: " + info + ", prop: " + prop + ", link: " + link;
        }
    }

    /**
     * LINK: info(1) + prop(4) + from_node(4) + to_node(4) + from_node_prev(4) + from_node_next(4) + to_node_prev(4) + to_node_next(4) #29Bytes
     *
     * @author ychao
     * @version $Revision: $, $Author: $, $Date: $
     */
    public static class L extends Item
    {
        public final static int LENGTH = 29;

        public final static int OFFSET_INFO = 0;

        public final static int OFFSET_PROP = OFFSET_INFO + XDB.BYTE_SIZE;

        public final static int OFFSET_FNODE = OFFSET_PROP + XDB.INT_SIZE;

        public final static int OFFSET_TNODE = OFFSET_FNODE + XDB.INT_SIZE;

        public final static int OFFSET_FNODE_PREV = OFFSET_TNODE + XDB.INT_SIZE;

        public final static int OFFSET_FNODE_NEXT = OFFSET_FNODE_PREV + XDB.INT_SIZE;

        public final static int OFFSET_TNODE_PREV = OFFSET_FNODE_NEXT + XDB.INT_SIZE;

        public final static int OFFSET_TNODE_NEXT = OFFSET_TNODE_PREV + XDB.INT_SIZE;

        public int prop;

        public int fnode;

        public int tnode;

        public int fnodePrev;

        public int fnodeNext;

        public int tnodePrev;

        public int tnodeNext;

        public L fill(byte info, int prop, int fnode, int tnode, int fnodePrev, int fnodeNext, int tnodePrev,
                int tnodeNext)
        {
            this.info = info;
            this.prop = prop;
            this.fnode = fnode;
            this.tnode = tnode;
            this.fnodePrev = fnodePrev;
            this.fnodeNext = fnodeNext;
            this.tnodePrev = tnodePrev;
            this.tnodeNext = tnodeNext;
            return this;
        }

        @Override
        public int length()
        {
            return LENGTH;
        }

        @Override
        public void encode(byte[] bytes, int offset)
        {
            ByteUtil.setByte(bytes, offset + OFFSET_INFO, info);
            ByteUtil.setInt(bytes, offset + OFFSET_PROP, prop);
            ByteUtil.setInt(bytes, offset + OFFSET_FNODE, fnode);
            ByteUtil.setInt(bytes, offset + OFFSET_TNODE, tnode);
            ByteUtil.setInt(bytes, offset + OFFSET_FNODE_PREV, fnodePrev);
            ByteUtil.setInt(bytes, offset + OFFSET_FNODE_NEXT, fnodeNext);
            ByteUtil.setInt(bytes, offset + OFFSET_TNODE_PREV, tnodePrev);
            ByteUtil.setInt(bytes, offset + OFFSET_TNODE_NEXT, tnodeNext);
        }

        @Override
        public void decode(byte[] bytes, int offset)
        {
            info = ByteUtil.getByte(bytes, offset + OFFSET_INFO);
            prop = ByteUtil.getInt(bytes, offset + OFFSET_PROP);
            fnode = ByteUtil.getInt(bytes, offset + OFFSET_FNODE);
            tnode = ByteUtil.getInt(bytes, offset + OFFSET_TNODE);
            fnodePrev = ByteUtil.getInt(bytes, offset + OFFSET_FNODE_PREV);
            fnodeNext = ByteUtil.getInt(bytes, offset + OFFSET_FNODE_NEXT);
            tnodePrev = ByteUtil.getInt(bytes, offset + OFFSET_TNODE_PREV);
            tnodeNext = ByteUtil.getInt(bytes, offset + OFFSET_TNODE_NEXT);
        }

        @Override
        public String toString()
        {
            return "info: " + info + ", prop: " + prop + ", fnode: " + fnode + ", tnode: " + tnode
                    + ", fnodePrev: " + fnodePrev + ", fnodeNext: " + fnodeNext + ", tnodePrev: " + tnodePrev
                    + ", tnodeNext: " + tnodeNext;
        }
    }

    /**
     * PROP: info(1) + prop_key(4) + prop_value(8) + next_prop(4) + nl(4) #21Bytes
     *         |
     *         |- [xxxx,----] #free...
     *         |- [----,xxxx] #data type
     *         
     * @author ychao
     * @version $Revision: $, $Author: $, $Date: $
     */
    public static class P extends Item
    {
        public final static int LENGTH = 21;

        public final static int OFFSET_INFO = 0;

        public final static int OFFSET_KEY = OFFSET_INFO + XDB.BYTE_SIZE;

        public final static int OFFSET_VALUE = OFFSET_KEY + XDB.INT_SIZE;

        public final static int OFFSET_NEXT = OFFSET_VALUE + XDB.LONG_SIZE;

        public final static int OFFSET_NL = OFFSET_NEXT + XDB.INT_SIZE;

        public final static byte VALUE_TYPE_MASK = 0x0F;

        public int key;

        public long value;

        public int next;

        public int nl;

        public P fill(byte info, int key, long value, int next, int nl)
        {
            this.info = info;
            this.key = key;
            this.value = value;
            this.next = next;
            this.nl = nl;
            return this;
        }

        public byte getValueType()
        {
            return (byte)(info & VALUE_TYPE_MASK);
        }

        @Override
        public int length()
        {
            return LENGTH;
        }

        @Override
        public void encode(byte[] bytes, int offset)
        {
            ByteUtil.setByte(bytes, offset + OFFSET_INFO, info);
            ByteUtil.setInt(bytes, offset + OFFSET_KEY, key);
            ByteUtil.setLong(bytes, offset + OFFSET_VALUE, value);
            ByteUtil.setInt(bytes, offset + OFFSET_NEXT, next);
            ByteUtil.setInt(bytes, offset + OFFSET_NL, nl);
        }

        @Override
        public void decode(byte[] bytes, int offset)
        {
            info = ByteUtil.getByte(bytes, offset + OFFSET_INFO);
            key = ByteUtil.getInt(bytes, offset + OFFSET_KEY);
            value = ByteUtil.getLong(bytes, offset + OFFSET_VALUE);
            next = ByteUtil.getInt(bytes, offset + OFFSET_NEXT);
            nl = ByteUtil.getInt(bytes, offset + OFFSET_NL);
        }

        @Override
        public String toString()
        {
            return "info: " + info + ", key: " + key + ", value: " + value + ", next: " + next + ", nl: "
                    + nl;
        }
    }
}
