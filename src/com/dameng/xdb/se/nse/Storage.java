/*
 * @(#)Storage.java, 2018年9月10日 下午10:05:11
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.nse;

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

/**
 * native storage implements
 * 
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Storage implements IStorage
{
    public static NLPStore nstore = new NLPStore();

    public static NLPStore lstore = new NLPStore();

    public static NLPStore pstore = new NLPStore();

    public static VStore vstore = new VStore();

    public static LTKStore ltkstore = new LTKStore();

    private int[] nebi, lebi, pebi; // record the session bind extent, and for array cache, used by 'put' only

    private Processor processor;

    public Storage(Processor processor)
    {
        this.processor = processor;
    }

    @Override
    public void initialize()
    {
        int extentCapacity = (int)Math.pow(2, XDB.Config.SE_EBI_BITS.value[0]);

        this.nebi = new int[] {(int)(processor.session.id % extentCapacity), 0, 0};
        this.lebi = new int[] {(int)(processor.session.id % extentCapacity), 0, 0};
        this.pebi = new int[] {(int)(processor.session.id % extentCapacity), 0, 0};
    }

    @Override
    public void destory()
    {}

    @Override
    public int[] putNodes(Node[] nodes)
    {
        NLPStore.N n = new NLPStore.N();

        int[] rets = new int[nodes.length];
        for (int i = 0; i < nodes.length; ++i)
        {
            rets[i] = nstore.alloc(nebi);
            nstore.put(rets[i], n.fill(FREE_FALSE, putGObject(nodes[i], rets[i]), ID_NULL));
        }

        return rets;
    }

    @Override
    public int[] putLinks(Link[] links)
    {
        int[] rets = new int[links.length];

        NLPStore.L l = new NLPStore.L();
        NLPStore.N fn = new NLPStore.N();
        NLPStore.N tn = new NLPStore.N();

        for (int i = 0; i < links.length; ++i)
        {
            if (!nstore.get(links[i].fnode, fn))
            {
                XDBException.SE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].fnode));
            }

            if (!nstore.get(links[i].tnode, tn))
            {
                XDBException.SE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].tnode));
            }

            rets[i] = lstore.alloc(lebi);
            lstore.put(rets[i], l.fill(FREE_FALSE, putGObject(links[i], rets[i]), links[i].fnode,
                    links[i].tnode, adjustLinkForLinkPut(links[i].fnode, fn, rets[i]), ID_NULL,
                    adjustLinkForLinkPut(links[i].tnode, tn, rets[i]), ID_NULL));
        }

        return rets;
    }

    @Override
    public Node[] getNodes(int[] ids)
    {
        NLPStore.N n = new NLPStore.N();

        Node[] nodes = new Node[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            if (!nstore.get(ids[i], n))
            {
                nodes[i] = null;
                continue;
            }

            nodes[i] = new Node(ids[i]);
            nodes[i].link = n.link;

            getGObject(nodes[i], n.prop);
        }

        return nodes;
    }

    @Override
    public Link[] getLinks(int[] ids)
    {
        NLPStore.L l = new NLPStore.L();

        Link[] links = new Link[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            if (!lstore.get(ids[i], l))
            {
                links[i] = null;
                continue;
            }

            links[i] = new Link(ids[i]);
            links[i].fnode = l.fnode;
            links[i].tnode = l.tnode;

            getGObject(links[i], l.prop);
        }

        return links;
    }

    @Override
    public boolean[] removeNode(int[] ids)
    {
        NLPStore.N n = new NLPStore.N();
        NLPStore.L l = new NLPStore.L();

        boolean[] rets = new boolean[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            rets[i] = nstore.remove(ids[i], n);
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
                lstore.remove(linkId, l);
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

        return rets;
    }

    @Override
    public boolean[] removeLink(int[] ids)
    {
        NLPStore.L l = new NLPStore.L();

        boolean[] rets = new boolean[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            rets[i] = lstore.remove(ids[i], l);
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

        return rets;
    }

    @Override
    public boolean[] setNode(Node[] nodes)
    {
        NLPStore.N n = new NLPStore.N();

        boolean[] rets = new boolean[nodes.length];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = nstore.get(nodes[i].id, n);
            if (!rets[i])
            {
                continue;
            }

            rets[i] = true;

            // remove original props
            removeGObject(n.prop);

            // set new props
            n.prop = putGObject(nodes[i], nodes[i].id);
            nstore.put(nodes[i].id, n);
        }

        return rets;
    }

    @Override
    public boolean[] setLink(Link[] links)
    {
        NLPStore.L l = new NLPStore.L();

        boolean[] rets = new boolean[links.length];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = lstore.get(links[i].id, l);
            if (!rets[i])
            {
                continue;
            }

            // remove original props
            removeGObject(l.prop);

            // set new props
            l.prop = putGObject(links[i], links[i].id);
            lstore.put(links[i].id, l);
        }

        return rets;
    }

    @Override
    public Node[] showNodes(int count)
    {
        List<NLPStore.N> nlist = new ArrayList<>(count);
        List<Integer> idList = nstore.show(true, count, nlist);

        Node[] nodes = new Node[nlist.size()];
        for (int i = 0; i < nodes.length; ++i)
        {
            nodes[i] = new Node(idList.get(i));
            nodes[i].link = nlist.get(i).link;

            getGObject(nodes[i], nlist.get(i).prop);
        }

        return nodes;
    }

    @Override
    public Link[] showLinks(int count)
    {
        List<NLPStore.L> llist = new ArrayList<>(count);
        List<Integer> idList = lstore.show(false, count, llist);

        Link[] links = new Link[llist.size()];
        for (int i = 0; i < links.length; ++i)
        {
            links[i] = new Link(idList.get(i));
            links[i].fnode = llist.get(i).fnode;
            links[i].tnode = llist.get(i).tnode;

            getGObject(links[i], llist.get(i).prop);
        }

        return links;
    }

    private int putGObject(GObject<?> obj, int id)
    {
        // category -> ltk.store & prop.store
        int propId = ID_NULL;
        NLPStore.P p = new NLPStore.P();
        for (int i = 0; i < obj.categorys.length; ++i)
        {
            p.fill(FREE_FALSE, ID_NULL, ltkstore.put(obj.categorys[i]), propId, id);
            propId = pstore.alloc(pebi);
            pstore.put(propId, p);
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
                    propValue = vstore.put((String)value.value);
                    break;
            }

            p.fill((byte)(FREE_FALSE | value.type), ltkstore.put(key), propValue, propId, id);
            propId = pstore.alloc(pebi);
            pstore.put(propId, p);
        }

        return propId;
    }

    private void getGObject(GObject<?> obj, int propId)
    {
        NLPStore.P p = new NLPStore.P();

        List<String> categoryList = new ArrayList<String>();
        do
        {
            pstore.get(propId, p);
            if (p.key == ID_NULL)
            {
                categoryList.add(ltkstore.get((int)p.value));
            }
            else
            {
                switch (p.valueType())
                {
                    case PropValue.TYPE_NUMBERIC:
                        obj.set(ltkstore.get(p.key), p.value);
                        break;
                    case PropValue.TYPE_DECIMAL:
                        obj.set(ltkstore.get(p.key), Double.longBitsToDouble(p.value));
                        break;
                    case PropValue.TYPE_BOOLEAN:
                        obj.set(ltkstore.get(p.key), p.value == 0 ? false : true);
                        break;
                    default:
                        obj.set(ltkstore.get(p.key), vstore.get(p.value));
                        break;
                }
            }
            propId = p.next;
        } while (p.next != ID_NULL);

        obj.categorys = categoryList.toArray(new String[0]);
    }

    private void removeGObject(int propId)
    {
        NLPStore.P p = new NLPStore.P();
        while (propId != ID_NULL)
        {
            pstore.remove(propId, p);
            if (p.valueType() == PropValue.TYPE_STRING)
            {
                vstore.remove(p.value);
            }
            propId = p.next;
        }
    }

    private int adjustLinkForLinkPut(int nodeId, NLPStore.N n, int linkId)
    {
        int prev = n.link;

        // first link
        if (n.link == ID_NULL)
        {
            n.link = linkId;
            nstore.put(nodeId, n);
            return prev;
        }

        // adjust node last link
        NLPStore.L l = new NLPStore.L();
        do
        {
            lstore.get(prev, l);
            if (l.fnode == nodeId)
            {
                if (l.fnode_next == ID_NULL)
                {
                    l.fnode_next = linkId;
                    lstore.put(prev, l);
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
                    lstore.put(prev, l);
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

    private void adjustLinkForLinkRemove(int nodeId, NLPStore.L l)
    {
        boolean isfnode = (nodeId == l.fnode);

        // adjust previous link
        int prev = isfnode ? l.fnode_prev : l.tnode_prev;
        if (prev != ID_NULL)
        {
            NLPStore.L tl = new NLPStore.L();
            lstore.get(prev, tl);
            if (tl.fnode == l.fnode)
            {
                tl.fnode_next = isfnode ? l.fnode_next : l.tnode_next;
            }
            else
            {
                tl.tnode_next = isfnode ? l.fnode_next : l.tnode_next;
            }
            lstore.put(prev, tl);
        }

        // adjust next link
        int next = isfnode ? l.fnode_next : l.tnode_next;
        if (next != ID_NULL)
        {
            NLPStore.L tl = new NLPStore.L();
            lstore.get(next, tl);
            if (tl.fnode == l.fnode)
            {
                tl.fnode_prev = isfnode ? l.fnode_prev : l.tnode_prev;
            }
            else
            {
                tl.tnode_prev = isfnode ? l.fnode_prev : l.tnode_prev;
            }
            lstore.put(l.fnode_next, tl);
        }

        // adjust node(remove link is the only one)
        if (prev == ID_NULL)
        {
            NLPStore.N n = new NLPStore.N();
            nstore.get(nodeId, n);
            n.link = next;
            nstore.put(nodeId, n);
        }
    }

    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();

        System.out.println("escape: " + (System.currentTimeMillis() - start));
    }
}
