/*
 * @(#)NLPStore.java, 2018年9月20日 下午2:03:34
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
import com.dameng.xdb.util.ByteUtil;

/**
 * used to store: node/link/property
 * 
 * NODE: info(1) + prop(4) + link(4) #9Bytes
 * 
 * LINK: info(1) + prop(4) + from_node(4) + to_node(4) + from_node_prev(4) + from_node_next(4) + to_node_prev(4) + to_node_next(4) #29Bytes
 * 
 * PROP: info(1) + prop_key(4) + prop_value(8) + next_prop(4) + nl(4) #21Bytes
 * 
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class NLPStore
{
    public final static byte FREE_MASK = (byte)0x80;

    public final static byte FREE_TRUE = (byte)0x00;

    public final static byte FREE_FALSE = (byte)0x80;

    public final byte EBITS, BBITS, IBITS;

    public final int EXTENTS, BLOCKS, ITEMS;

    public Extent[] extents;

    /**
     * ebits + bbits + ibits = 32
     * 
     * @param ebits: extent number need bits
     * @param bbits: block number need bits
     * @param ibits: item number need bits
     * @param ilen:  item record length(B)
     */
    public NLPStore(byte ebits, byte bbits, byte ibits)
    {
        this.EBITS = ebits;
        this.BBITS = bbits;
        this.IBITS = ibits;

        this.EXTENTS = (int)Math.pow(2, ebits);
        this.BLOCKS = (int)Math.pow(2, bbits);
        this.ITEMS = (int)Math.pow(2, ibits);

        this.extents = new Extent[EXTENTS];
        for (int e = 0; e < EXTENTS; ++e)
        {
            this.extents[e] = new Extent(e);
        }
    }
    
    @Override
    public String toString()
    {
         StringBuilder str = new StringBuilder();
         for (int e = 0; e < EXTENTS; ++e)
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

    public boolean read(int id, StoreItem item)
    {
        int[] ebi = id2ebi(id);
        return extents[ebi[0]].blocks[ebi[1]].read(ebi[2], item);
    }
    
    public void write(int id, StoreItem item)
    {
        int[] ebi = id2ebi(id);
        extents[ebi[0]].blocks[ebi[1]].write(ebi[2], item);
    }

    public boolean free(int id, StoreItem item)
    {
        int[] ebi = id2ebi(id);
        return extents[ebi[0]].blocks[ebi[1]].free(ebi[2], item);
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
                while (extent.offset < BLOCKS)
                {
                    block = extent.blocks[extent.offset];
                    if (block.offset < ITEMS)
                    {
                        ebi[1] = extent.offset;
                        ebi[2] = block.offset;
                        block.offset++;
                        return ebi2id(ebi);
                    }

                    extent.offset++;
                }
            }
        } while ((ebi[0] = (ebi[0]++) % EXTENTS) != e);

        throw XDBException.SE_NO_MORE_SPACE;
    }

    @SuppressWarnings ("unchecked")
    public <T extends StoreItem> List<Integer> show(boolean node, int count, List<T> itemList)
    {
        List<Integer> idList = new ArrayList<Integer>();
        StoreItem item = node ? new Node() : new Link();

        exit: for (int e = 0; e < EXTENTS; ++e)
        {
            Extent extent = extents[e];

            synchronized (extent)
            {
                for (int b = 0; b <= extent.offset; ++b)
                {
                    Block block = extent.blocks[b];
                    for (int i = 0; i < block.offset; ++i)
                    {
                        block.read(i, item);
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

                        item = node ? new Node() : new Link();
                    }
                }
            }
        }

        return idList;
    }

    class Extent
    {
        public int id;

        public Block[] blocks;

        public int offset; // block offset of extent [0, BLOCKS]

        public Extent(int id)
        {
            this.id = id;
            this.offset = 0;
            this.blocks = new Block[BLOCKS];
            for (int b = 0; b < BLOCKS; ++b)
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
            for (int b = 0; b < BLOCKS; ++b)
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

        public int offset; // item offset of block [0, ITEMS]

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

        public boolean read(int i, StoreItem item)
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
        
        public void write(int i, StoreItem item)
        {
            try
            {
                lock.writeLock().lock();

                if (bytes == null)
                {
                    bytes = new byte[ITEMS * item.length()];
                }

                item.encode(bytes, i * item.length());
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }

        public boolean free(int i, StoreItem item)
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

    public static abstract class StoreItem
    {
        public byte info;

        public StoreItem()
        {}

        public void free()
        {
            info = (byte)(info & ~FREE_MASK | FREE_TRUE);
        }

        public boolean isFree()
        {
            return (info & FREE_MASK) == FREE_TRUE;
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
    public static class Node extends StoreItem
    {
        public final static int LENGTH = 9;

        public final static int OFFSET_INFO = 0;

        public final static int OFFSET_PROP = OFFSET_INFO + XDB.BYTE_SIZE;

        public final static int OFFSET_LINK = OFFSET_PROP + XDB.INT_SIZE;

        public int prop;

        public int link;

        public void set(byte info, int prop, int link)
        {
            this.info = info;
            this.prop = prop;
            this.link = link;
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
    public static class Link extends StoreItem
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

        public void set(byte info, int prop, int fnode, int tnode, int fnodePrev, int fnodeNext,
                int tnodePrev, int tnodeNext)
        {
            this.info = info;
            this.prop = prop;
            this.fnode = fnode;
            this.tnode = tnode;
            this.fnodePrev = fnodePrev;
            this.fnodeNext = fnodeNext;
            this.tnodePrev = tnodePrev;
            this.tnodeNext = tnodeNext;
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
    public static class Prop extends StoreItem
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

        public void set(byte info, int key, long value, int next, int nl)
        {
            this.info = info;
            this.key = key;
            this.value = value;
            this.next = next;
            this.nl = nl;
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
