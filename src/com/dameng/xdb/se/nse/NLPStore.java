/*
 * @(#)NLPStore.java, 2018年9月20日 下午9:03:34
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.nse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.IStorage;
import com.dameng.xdb.util.ByteUtil;

/**
 * Node/Link/Prop storage
 * 
 * Node: info(1) + prop(4) + link(4) #9Bytes
 * 
 * Link: info(1) + prop(4) + from_node(4) + to_node(4) + from_node_prev(4) + from_node_next(4) + to_node_prev(4) + to_node_next(4) #29Bytes
 * 
 * Prop: info(1) + prop_key(4) + prop_value(8) + next_prop(4) + nl(4) #21Bytes
 * 
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class NLPStore extends Store
{
    private final int EBITS = XDB.Config.SE_EBI_BITS.value[0];
    
    private final int BBITS = XDB.Config.SE_EBI_BITS.value[1];
    
    private final int IBITS = XDB.Config.SE_EBI_BITS.value[2];

    public int capacity = (int)Math.pow(2, EBITS);
    
    public Extent[] extents;
    
    public NLPStore()
    {
        this.extents = new Extent[capacity];
        for (int e = 0; e < extents.length; ++e)
        {
            this.extents[e] = new Extent(e);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        for (int e = 0; e < capacity; ++e)
        {
            str.append(extents[e].toString());
            str.append("\n");
        }
        return str.toString();
    }

    private int ebi2id(int... ebi)
    {
        return ebi[0] << (BBITS + IBITS) | ebi[1] << IBITS | ebi[2];
    }

    private int[] id2ebi(int id)
    {
        int[] ebi = new int[3];
        ebi[0] = id >>> (BBITS + IBITS);
        ebi[1] = id << EBITS >>> (EBITS + IBITS);
        ebi[2] = id << (EBITS + BBITS) >>> (EBITS + BBITS);
        return ebi;
    }

    public boolean get(int id, Item item)
    {
        int[] ebi = id2ebi(id);
        return extents[ebi[0]].blocks[ebi[1]].get(ebi[2], item);
    }

    public void put(int id, Item item)
    {
        int[] ebi = id2ebi(id);
        extents[ebi[0]].blocks[ebi[1]].put(ebi[2], item);
    }

    public boolean remove(int id, Item item)
    {
        int[] ebi = id2ebi(id);
        return extents[ebi[0]].blocks[ebi[1]].remove(ebi[2], item);
    }

    public int alloc(int[] ebi)
    {
        int e = ebi[0];

        do
        {
            Extent extent = extents[ebi[0]];

            synchronized (extent)
            {
                Block block = null;
                while (extent.offset < extent.capacity)
                {
                    block = extent.blocks[extent.offset];
                    if (block.offset < block.capacity)
                    {
                        ebi[1] = extent.offset;
                        ebi[2] = block.offset;
                        block.offset++;
                        return ebi2id(ebi);
                    }

                    extent.offset++;
                }
            }
        } while ((ebi[0] = (ebi[0]++) % capacity) != e);

        throw XDBException.SE_NO_MORE_SPACE;
    }

    @SuppressWarnings ("unchecked")
    public <T extends Item> List<Integer> show(boolean node, int count, List<T> itemList)
    {
        List<Integer> idList = new ArrayList<Integer>();
        Item item = node ? new N() : new L();

        exit: for (int e = 0; e < capacity; ++e)
        {
            Extent extent = extents[e];

            synchronized (extent)
            {
                for (int b = 0; b <= extent.offset; ++b)
                {
                    Block block = extent.blocks[b];
                    for (int i = 0; i < block.offset; ++i)
                    {
                        block.get(i, item);
                        if (item.isFree())
                        {
                            continue;
                        }

                        itemList.add((T)item);
                        idList.add(ebi2id(e, b, i));

                        if ((--count) == 0)
                        {
                            break exit;
                        }

                        item = node ? new N() : new L();
                    }
                }
            }
        }

        return idList;
    }

    /**
     * store item
     */
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
     * LINK: info(1) + prop(4) + from_node(4) + to_node(4) + fnode_prev(4) + fnode_next(4) + tnode_prev(4) + tnode_next(4) #29Bytes
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

        public int fnode_prev;

        public int fnode_next;

        public int tnode_prev;

        public int tnode_next;

        public L fill(byte info, int prop, int fnode, int tnode, int fnode_prev, int fnode_next, int tnode_prev,
                int tnode_next)
        {
            this.info = info;
            this.prop = prop;
            this.fnode = fnode;
            this.tnode = tnode;
            this.fnode_prev = fnode_prev;
            this.fnode_next = fnode_next;
            this.tnode_prev = tnode_prev;
            this.tnode_next = tnode_next;
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
            ByteUtil.setInt(bytes, offset + OFFSET_FNODE_PREV, fnode_prev);
            ByteUtil.setInt(bytes, offset + OFFSET_FNODE_NEXT, fnode_next);
            ByteUtil.setInt(bytes, offset + OFFSET_TNODE_PREV, tnode_prev);
            ByteUtil.setInt(bytes, offset + OFFSET_TNODE_NEXT, tnode_next);
        }

        @Override
        public void decode(byte[] bytes, int offset)
        {
            info = ByteUtil.getByte(bytes, offset + OFFSET_INFO);
            prop = ByteUtil.getInt(bytes, offset + OFFSET_PROP);
            fnode = ByteUtil.getInt(bytes, offset + OFFSET_FNODE);
            tnode = ByteUtil.getInt(bytes, offset + OFFSET_TNODE);
            fnode_prev = ByteUtil.getInt(bytes, offset + OFFSET_FNODE_PREV);
            fnode_next = ByteUtil.getInt(bytes, offset + OFFSET_FNODE_NEXT);
            tnode_prev = ByteUtil.getInt(bytes, offset + OFFSET_TNODE_PREV);
            tnode_next = ByteUtil.getInt(bytes, offset + OFFSET_TNODE_NEXT);
        }

        @Override
        public String toString()
        {
            return "info: " + info + ", prop: " + prop + ", fnode: " + fnode + ", tnode: " + tnode
                    + ", fnode_prev: " + fnode_prev + ", fnode_next: " + fnode_next + ", tnode_prev: " + tnode_prev
                    + ", tnode_next: " + tnode_next;
        }
    }

    /**
     * PROP: info(1) + prop_key(4) + prop_value(8) + next_prop(4) + nl(4) #21Bytes
     *         |
     *         |- [xxxx,----] #free...
     *         |- [----,xxxx] #data type
     */
    public static class P extends Item
    {
        public final static int LENGTH = 21;

        public final static int OFFSET_INFO = 0;

        public final static int OFFSET_KEY = OFFSET_INFO + XDB.BYTE_SIZE;

        public final static int OFFSET_VALUE = OFFSET_KEY + XDB.INT_SIZE;

        public final static int OFFSET_NEXT = OFFSET_VALUE + XDB.LONG_SIZE;

        public final static int OFFSET_NL = OFFSET_NEXT + XDB.INT_SIZE;

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

        public byte valueType()
        {
            return (byte)(info & IStorage.VALUE_TYPE_MASK);
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

class Extent
{
    public int id;

    public Block[] blocks;

    public int offset; // block offset of extent [0, BLOCK_TOTAL)

    public int capacity = (int)Math.pow(2, XDB.Config.SE_EBI_BITS.value[1]);

    public Extent(int id)
    {
        this.id = id;
        this.offset = 0;
        this.blocks = new Block[capacity];
        for (int b = 0; b < capacity; ++b)
        {
            this.blocks[b] = new Block(b, id);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("(");
        str.append(String.format("%4d", id));
        str.append(",");
        str.append(String.format("%4d", offset));
        str.append(")");
        str.append("[");
        for (int b = 0; b < capacity; ++b)
        {
            str.append(blocks[b].toString());
        }
        str.append("]");
        return str.toString();
    }
}

class Block
{
    public int id;

    public int extId;

    public byte[] bytes;

    public int offset; // item offset of block [0, ITEM_TOTAL)

    public int capacity = (int)Math.pow(2, XDB.Config.SE_EBI_BITS.value[2]);

    public ReadWriteLock lock = new ReentrantReadWriteLock();

    public Block(int id, int extId)
    {
        this.id = id;
        this.extId = extId;
        this.offset = (id == 0 && extId == 0) ? 1 : 0; // id 0 is reserved, used as id null.
    }

    @Override
    public String toString()
    {
        return offset == 0 ? "-" : String.valueOf(offset);
    }

    public boolean get(int i, NLPStore.Item item)
    {
        try
        {
            lock.readLock().lock();

            if (bytes == null)
            {
                item.free();
            }
            else
            {
                item.decode(bytes, i * item.length());
            }

            return !item.isFree();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public void put(int i, NLPStore.Item item)
    {
        try
        {
            lock.writeLock().lock();

            if (bytes == null)
            {
                bytes = new byte[capacity * item.length()];
            }

            item.encode(bytes, i * item.length());
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public boolean remove(int i, NLPStore.Item item)
    {
        try
        {
            lock.writeLock().lock();

            if (bytes == null)
            {
                return false;
            }

            item.decode(bytes, i * item.length());
            if (item.isFree())
            {
                return false;
            }

            item.free();
            item.encode(bytes, i * item.length());

            return true;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}
