/*
 * @(#)Storage.java, 2018年12月12日 上午10:33:40
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.rdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.IStorage;
import com.dameng.xdb.se.Processor;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;
import com.dameng.xdb.se.model.PropValue;
import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Storage implements IStorage
{
    static
    {
        try
        {
            Class.forName(XDB.Config.SE_RDB_DRIVER.value);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("JDBC Driver(" + XDB.Config.SE_RDB_DRIVER + ") load fail!", e);
        }
    }

    @SuppressWarnings ("unused")
    private Processor processor;

    private Connection connection;

    private NStore nstore;

    private LStore lstore;

    private PStore pstore;

    private VStore vstore;

    private LTKStore ltkstore;

    public Storage(Processor processor)
    {
        this.processor = processor;
        try
        {
            this.connection = DriverManager.getConnection(XDB.Config.SE_RDB_URL.value,
                    XDB.Config.SE_RDB_USER.value, XDB.Config.SE_RDB_PASSWORD.value);
        }
        catch (SQLException e)
        {
            throw new RuntimeException("RDB connect fail, database is ready and configures are right?", e);
        }

        nstore = new NStore(this.connection);
        lstore = new LStore(this.connection);
        pstore = new PStore(this.connection);
        vstore = new VStore(this.connection);
        ltkstore = new LTKStore(this.connection);
    }

    @Override
    public void initialize()
    {
        try
        {
            nstore.initialize();
            lstore.initialize();
            pstore.initialize();
            vstore.initialize();
            ltkstore.initialize();
        }
        catch (Exception e)
        {
            throw new RuntimeException("RDB store initialize fail!", e);
        }
    }

    @Override
    public void destory()
    {
        try
        {
            nstore.destory();
            lstore.destory();
            pstore.destory();
            vstore.destory();
            ltkstore.destory();
        }
        catch (Exception e)
        {}

        MiscUtil.close(this.connection);
    }

    @Override
    public int[] putNodes(Node[] nodes)
    {
        NStore.N n = new NStore.N();

        int[] rets = new int[nodes.length];

        try
        {
            for (int i = 0; i < nodes.length; ++i)
            {
                rets[i] = nstore.put(n.fill(FREE_FALSE, putGObject(nodes[i]), ID_NULL));
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return rets;
    }

    @Override
    public int[] putLinks(Link[] links)
    {
        LStore.L l = new LStore.L();
        NStore.N fn = new NStore.N();
        NStore.N tn = new NStore.N();

        int[] rets = new int[links.length];

        try
        {
            for (int i = 0; i < links.length; ++i)
            {
                if (!nstore.get(fn.fill(links[i].fnode)))
                {
                    XDBException.SE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].fnode));
                }

                if (!nstore.get(tn.fill(links[i].tnode)))
                {
                    XDBException.SE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].tnode));
                }

                rets[i] = lstore.put(l.fill(FREE_FALSE, putGObject(links[i]), links[i].fnode,
                        links[i].tnode, ID_NULL, ID_NULL, ID_NULL, ID_NULL));
                l.fnode_prev = adjustLinkForLinkPut(rets[i], fn);
                l.tnode_prev = adjustLinkForLinkPut(rets[i], tn);
                lstore.set(l);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return rets;
    }

    @Override
    public Node[] getNodes(int[] ids)
    {
        Node[] nodes = new Node[ids.length];

        try
        {
            NStore.N n = new NStore.N();

            for (int i = 0; i < ids.length; ++i)
            {
                if (!nstore.get(n.fill(ids[i])))
                {
                    nodes[i] = null;
                    continue;
                }

                nodes[i] = new Node(ids[i]);
                nodes[i].link = n.link;

                getGObject(nodes[i], n.prop);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return nodes;
    }

    @Override
    public Link[] getLinks(int[] ids)
    {
        Link[] links = new Link[ids.length];

        try
        {
            LStore.L l = new LStore.L();
            for (int i = 0; i < ids.length; ++i)
            {
                if (!lstore.get(l.fill(ids[i])))
                {
                    links[i] = null;
                    continue;
                }

                links[i] = new Link(ids[i]);
                links[i].fnode = l.fnode;
                links[i].tnode = l.tnode;

                getGObject(links[i], l.prop);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return links;
    }

    @Override
    public boolean[] removeNode(int[] ids)
    {
        boolean[] rets = new boolean[ids.length];

        try
        {
            NStore.N n = new NStore.N();
            LStore.L l = new LStore.L();

            for (int i = 0; i < ids.length; ++i)
            {
                nstore.get(n.fill(ids[i]));
                rets[i] = nstore.remove(n);
                if (!rets[i])
                {
                    continue;
                }

                // remove node props
                removeGObject(n.prop);

                // adjust relate links
                int linkId = n.link;
                while (linkId != ID_NULL)
                {
                    lstore.get(l.fill(linkId));
                    lstore.remove(l);
                    removeGObject(l.prop);
                    if (l.fnode == ids[i])
                    {
                        adjustLinkForLinkRemove(l.tnode, l);
                        linkId = l.fnode_next;
                    }
                    else
                    {
                        adjustLinkForLinkRemove(l.fnode, l);
                        linkId = l.tnode_next;
                    }
                }
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return rets;
    }

    @Override
    public boolean[] removeLink(int[] ids)
    {
        boolean[] rets = new boolean[ids.length];

        try
        {
            LStore.L l = new LStore.L();

            for (int i = 0; i < ids.length; ++i)
            {
                lstore.get(l.fill(ids[i]));
                rets[i] = lstore.remove(l);
                if (!rets[i])
                {
                    continue;
                }

                // remove link props
                removeGObject(l.prop);

                // adjust relate links
                adjustLinkForLinkRemove(l.fnode, l);
                adjustLinkForLinkRemove(l.tnode, l);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return rets;
    }

    @Override
    public boolean[] setNode(Node[] nodes)
    {
        boolean[] rets = new boolean[nodes.length];

        try
        {
            NStore.N n = new NStore.N();

            for (int i = 0; i < rets.length; ++i)
            {
                rets[i] = nstore.get(n.fill(nodes[i].id));
                if (!rets[i])
                {
                    continue;
                }

                rets[i] = true;

                // remove original props
                removeGObject(n.prop);

                // set new props
                n.prop = putGObject(nodes[i]);
                nstore.set(n);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return rets;
    }

    @Override
    public boolean[] setLink(Link[] links)
    {
        boolean[] rets = new boolean[links.length];

        try
        {
            LStore.L l = new LStore.L();

            for (int i = 0; i < rets.length; ++i)
            {
                rets[i] = lstore.get(l.fill(links[i].id));
                if (!rets[i])
                {
                    continue;
                }

                // remove original props
                removeGObject(l.prop);

                // set new props
                l.prop = putGObject(links[i]);
                lstore.set(l);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return rets;
    }

    @Override
    public Node[] showNodes(int count)
    {
        Node[] nodes = null;

        try
        {
            NStore.N n = new NStore.N();
            List<NStore.N> nlist = nstore.show((NStore.N)n.fill(count));

            nodes = new Node[nlist.size()];
            for (int i = 0; i < nodes.length; ++i)
            {
                nodes[i] = new Node(nlist.get(i).id);
                nodes[i].link = nlist.get(i).link;

                getGObject(nodes[i], nlist.get(i).prop);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return nodes;
    }

    @Override
    public Link[] showLinks(int count)
    {
        Link[] links = null;

        try
        {
            LStore.L l = new LStore.L();
            List<LStore.L> llist = lstore.show((LStore.L)l.fill(count));

            links = new Link[llist.size()];
            for (int i = 0; i < links.length; ++i)
            {
                links[i] = new Link(llist.get(i).id);
                links[i].fnode = llist.get(i).fnode;
                links[i].tnode = llist.get(i).tnode;

                getGObject(links[i], llist.get(i).prop);
            }
        }
        catch (Exception e)
        {
            XDBException.SE_RDB_ACCESS_ERROR.throwException(e);
        }

        return links;
    }

    private int putGObject(GObject<?> obj) throws Exception
    {
        PStore.P p = new PStore.P();
        VStore.V v = new VStore.V();
        LTKStore.LTK ltk = new LTKStore.LTK();

        // category -> ltk.store & prop.store
        int propId = ID_NULL;
        for (int i = 0; i < obj.categorys.length; ++i)
        {
            ltkstore.put(ltk.fill(obj.categorys[i]));
            pstore.put(p.fill(FREE_FALSE, ID_NULL, ltk.id, propId));
            propId = p.id;
        }

        // properties -> ltk.store & prop.store
        Iterator<Entry<String, PropValue>> iterator = obj.propMap.entrySet().iterator();
        while (iterator.hasNext())
        {
            Entry<String, PropValue> entry = iterator.next();
            String key = entry.getKey();
            PropValue value = entry.getValue();

            // value
            long propValue = 0;
            switch (value.type)
            {
                case PropValue.TYPE_NUMBERIC:
                    propValue = (long)value.value;
                    break;
                case PropValue.TYPE_DECIMAL:
                    propValue = Double.doubleToLongBits((double)value.value);
                    break;
                case PropValue.TYPE_BOOLEAN:
                    propValue = (boolean)value.value ? 1 : 0;
                    break;
                default:
                    vstore.put(v.fill((String)value.value));
                    propValue = v.id;
                    break;
            }

            ltkstore.put(ltk.fill(key));
            pstore.put(p.fill((byte)(FREE_FALSE | value.type), ltk.id, propValue, propId));
            propId = p.id;
        }

        return propId;
    }

    private void getGObject(GObject<?> obj, int propId) throws Exception
    {
        PStore.P p = new PStore.P();
        VStore.V v = new VStore.V();
        LTKStore.LTK ltk = new LTKStore.LTK();

        List<String> categoryList = new ArrayList<String>();
        do
        {
            pstore.get(p.fill(propId));
            if (p.key == ID_NULL)
            {
                ltkstore.get(ltk.fill((int)p.value));
                categoryList.add(ltk.value);
            }
            else
            {
                ltkstore.get(ltk.fill(p.key));
                switch (p.valueType())
                {
                    case PropValue.TYPE_NUMBERIC:
                        obj.set(ltk.value, p.value);
                        break;
                    case PropValue.TYPE_DECIMAL:
                        obj.set(ltk.value, Double.longBitsToDouble(p.value));
                        break;
                    case PropValue.TYPE_BOOLEAN:
                        obj.set(ltk.value, p.value == 0 ? false : true);
                        break;
                    default:
                        vstore.get(v.fill((int)p.value));
                        obj.set(ltk.value, v.value);
                        break;
                }
            }
            propId = p.next;
        } while (p.next != ID_NULL);

        obj.categorys = categoryList.toArray(new String[0]);
    }

    private void removeGObject(int propId) throws Exception
    {
        PStore.P p = new PStore.P();
        VStore.V v = new VStore.V();

        while (propId != ID_NULL)
        {
            pstore.get(p.fill(propId));
            pstore.remove(p);
            if (p.valueType() == PropValue.TYPE_STRING)
            {
                vstore.remove(v.fill((int)p.value));
            }
            propId = p.next;
        }
    }

    private int adjustLinkForLinkPut(int linkId, NStore.N n) throws Exception
    {
        if (n.link == ID_NULL)
        {
            n.link = linkId;
            nstore.set(n);
            return ID_NULL;
        }

        // adjust node last link
        int prev = n.link;
        LStore.L l = new LStore.L();
        do
        {
            lstore.get(l.fill(prev));
            if (l.fnode == n.id)
            {
                if (l.fnode_next == ID_NULL)
                {
                    l.fnode_next = linkId;
                    lstore.set(l);
                    break;
                }
                else
                {
                    prev = l.fnode_next;
                }
            }
            else
            {
                if (l.tnode_next == ID_NULL)
                {
                    l.tnode_next = linkId;
                    lstore.set(l);
                    break;
                }
                else
                {
                    prev = l.tnode_next;
                }
            }
        } while (true);

        return prev;
    }

    private void adjustLinkForLinkRemove(int nodeId, LStore.L l) throws Exception
    {
        boolean isfnode = (nodeId == l.fnode);

        // adjust previous link
        int prev = isfnode ? l.fnode_prev : l.tnode_prev;
        if (prev != ID_NULL)
        {
            LStore.L tl = new LStore.L();
            lstore.get(tl.fill(prev));
            if (tl.fnode == l.fnode)
            {
                tl.fnode_next = isfnode ? l.fnode_next : l.tnode_next;
            }
            else
            {
                tl.tnode_next = isfnode ? l.fnode_next : l.tnode_next;
            }
            lstore.set(tl);
        }

        // adjust next link
        int next = isfnode ? l.fnode_next : l.tnode_next;
        if (next != ID_NULL)
        {
            LStore.L tl = new LStore.L();
            lstore.get(tl.fill(next));
            if (tl.fnode == l.fnode)
            {
                tl.fnode_prev = isfnode ? l.fnode_prev : l.tnode_prev;
            }
            else
            {
                tl.tnode_prev = isfnode ? l.fnode_prev : l.tnode_prev;
            }
            lstore.set(tl.fill(l.fnode_next));
        }

        // adjust node(remove link is the only one)
        if (prev == ID_NULL)
        {
            NStore.N n = new NStore.N();
            nstore.get(n.fill(nodeId));
            n.link = next;
            nstore.set(n);
        }
    }
}
