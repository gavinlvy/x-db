/*
 * @(#)BaseStore.java, 2019年4月14日 下午10:12:17
 *
 * Copyright (c) 2000-2019, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.rdb;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Store
{
    public void destory()
    {
        // implements by subclass
    }

    public static class N
    {
        public byte info;

        public int prop;

        public int link;

        public N fill(byte info, int prop, int link)
        {
            this.info = info;
            this.prop = prop;
            this.link = link;
            return this;
        }
    }

    public static class L
    {
        public byte info;

        public int prop;

        public int fnode;

        public int tnode;

        public int fnode_prev;

        public int fnode_next;

        public int tnode_prev;

        public int tnode_next;

        public L fill(byte info, int prop, int fnode, int tnode, int fnode_prev, int fnode_next,
                int tnode_prev, int tnode_next)
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
    }

    public static class P
    {
        public byte info;

        public int key;

        public long value;

        public int next;

        public P fill(byte info, int key, long value, int next)
        {
            this.info = info;
            this.key = key;
            this.value = value;
            this.next = next;
            return this;
        }
    }

    public static class V
    {
        public String value;

        public V fill(String value)
        {
            this.value = value;
            return this;
        }
    }

    public static class LTK
    {
        public String value;

        public LTK fill(String value)
        {
            this.value = value;
            return this;
        }
    }
}
