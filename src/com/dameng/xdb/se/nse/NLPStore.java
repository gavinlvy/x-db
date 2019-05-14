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

import com.dameng.xdb.XDBException;

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
    public final byte EBITS, BBITS, IBITS;

    public final int EXTENT_TOTAL, BLOCK_TOTAL, ITEM_TOTAL;

    public Extent[] extents;

    /**
     * ebits + bbits + ibits = 32
     * 
     * @param ebits: extent number need bits
     * @param bbits: block number need bits
     * @param ibits: item number need bits
     */
    public NLPStore(byte ebits, byte bbits, byte ibits)
    {
        this.EBITS = ebits;
        this.BBITS = bbits;
        this.IBITS = ibits;

        this.EXTENT_TOTAL = (int)Math.pow(2, ebits);
        this.BLOCK_TOTAL = (int)Math.pow(2, bbits);
        this.ITEM_TOTAL = (int)Math.pow(2, ibits);

        this.extents = new Extent[EXTENT_TOTAL];
        for (int e = 0; e < EXTENT_TOTAL; ++e)
        {
            this.extents[e] = new Extent(e);
        }
    }
    
    @Override
    public String toString()
    {
         StringBuilder str = new StringBuilder();
         for (int e = 0; e < EXTENT_TOTAL; ++e)
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

    public boolean read(int id, Item item)
    {
        int[] ebi = id2ebi(id);
        return extents[ebi[0]].blocks[ebi[1]].read(ebi[2], item);
    }
    
    public void write(int id, Item item)
    {
        int[] ebi = id2ebi(id);
        extents[ebi[0]].blocks[ebi[1]].write(ebi[2], item);
    }

    public boolean free(int id, Item item)
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
                while (extent.offset < BLOCK_TOTAL)
                {
                    block = extent.blocks[extent.offset];
                    if (block.offset < ITEM_TOTAL)
                    {
                        ebi[1] = extent.offset;
                        ebi[2] = block.offset;
                        block.offset++;
                        return ebi2id(ebi);
                    }

                    extent.offset++;
                }
            }
        } while ((ebi[0] = (ebi[0]++) % EXTENT_TOTAL) != e);

        throw XDBException.SE_NO_MORE_SPACE;
    }

    @SuppressWarnings ("unchecked")
    public <T extends Item> List<Integer> show(boolean node, int count, List<T> itemList)
    {
        List<Integer> idList = new ArrayList<Integer>();
        Item item = node ? new N() : new L();

        exit: for (int e = 0; e < EXTENT_TOTAL; ++e)
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

                        item = node ? new N() : new L();
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

        public int offset; // block offset of extent [0, BLOCK_TOTAL)

        public Extent(int id)
        {
            this.id = id;
            this.offset = 0;
            this.blocks = new Block[BLOCK_TOTAL];
            for (int b = 0; b < BLOCK_TOTAL; ++b)
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
            for (int b = 0; b < BLOCK_TOTAL; ++b)
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

        public boolean read(int i, Item item)
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
        
        public void write(int i, Item item)
        {
            try
            {
                lock.writeLock().lock();

                if (bytes == null)
                {
                    bytes = new byte[ITEM_TOTAL * item.length()];
                }

                item.encode(bytes, i * item.length());
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }

        public boolean free(int i, Item item)
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
}
