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
    public final static NLPStore NSTORE = new NLPStore(XDB.Config.SE_EBI_BITS.value[0],
            XDB.Config.SE_EBI_BITS.value[1], XDB.Config.SE_EBI_BITS.value[2]);

    public final static NLPStore LSTORE = new NLPStore(XDB.Config.SE_EBI_BITS.value[0],
            XDB.Config.SE_EBI_BITS.value[1], XDB.Config.SE_EBI_BITS.value[2]);

    public final static NLPStore PSTORE = new NLPStore(XDB.Config.SE_EBI_BITS.value[0],
            XDB.Config.SE_EBI_BITS.value[1], XDB.Config.SE_EBI_BITS.value[2]);

    public final static VStore VSTORE = new VStore();

    public final static LTKStore LTKSTORE = new LTKStore();

    private int[] nebi, lebi, pebi; // record the session bind extent, and for array cache, used by 'put' only

    private Processor processor;

    public Storage(Processor processor)
    {
        this.processor = processor;
    }

    @Override
    public void initialize()
    {
        this.nebi = new int[] {(int)(processor.session.id % NSTORE.EXTENT_TOTAL), 0, 0};
        this.lebi = new int[] {(int)(processor.session.id % LSTORE.EXTENT_TOTAL), 0, 0};
        this.pebi = new int[] {(int)(processor.session.id % PSTORE.EXTENT_TOTAL), 0, 0};
    }

    @Override
    public void destory()
    {}

    @Override
    public int[] putNodes(Node[] nodes)
    {
        Store.N n = new Store.N();

        int[] rets = new int[nodes.length];
        for (int i = 0; i < nodes.length; ++i)
        {
            rets[i] = NSTORE.alloc(nebi);
            NSTORE.write(rets[i], n.fill(FREE_FALSE, putGObject(rets[i], nodes[i]), ID_NULL));
        }

        return rets;
    }

    @Override
    public int[] putLinks(Link[] links)
    {
        int[] rets = new int[links.length];

        Store.L l = new Store.L();
        Store.N fn = new Store.N();
        Store.N tn = new Store.N();

        for (int i = 0; i < links.length; ++i)
        {
            if (!NSTORE.read(links[i].fnode, fn))
            {
                XDBException.SE_NSE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].fnode));
            }

            if (!NSTORE.read(links[i].tnode, tn))
            {
                XDBException.SE_NSE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].tnode));
            }

            rets[i] = LSTORE.alloc(lebi);
            LSTORE.write(rets[i], l.fill(FREE_FALSE, putGObject(rets[i], links[i]), links[i].fnode,
                    links[i].tnode, adjustLinkForLinkPut(links[i].fnode, fn, rets[i]), ID_NULL,
                    adjustLinkForLinkPut(links[i].tnode, tn, rets[i]), ID_NULL));
        }

        return rets;
    }

    @Override
    public Node[] getNodes(int[] ids)
    {
        Store.N n = new Store.N();

        Node[] nodes = new Node[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            if (!NSTORE.read(ids[i], n))
            {
                nodes[i] = null;
                continue;
            }

            nodes[i] = new Node(ids[i]);
            nodes[i].link = n.link;

            getGObject(n.prop, nodes[i]);
        }

        return nodes;
    }

    @Override
    public Link[] getLinks(int[] ids)
    {
        Store.L l = new Store.L();

        Link[] links = new Link[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            if (!LSTORE.read(ids[i], l))
            {
                links[i] = null;
                continue;
            }

            links[i] = new Link(ids[i]);
            links[i].fnode = l.fnode;
            links[i].tnode = l.tnode;

            getGObject(l.prop, links[i]);
        }

        return links;
    }

    @Override
    public boolean[] removeNode(int[] ids)
    {
        Store.N n = new Store.N();
        Store.L l = new Store.L();

        boolean[] rets = new boolean[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            rets[i] = NSTORE.free(ids[i], n);
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
                LSTORE.free(linkId, l);
                removeGObject(l.prop);
                if (l.fnode == ids[i])
                {
                    adjustLinkForLinkRemove(l.tnode, l);
                    linkId = l.fnodeNext;
                }
                else
                {
                    adjustLinkForLinkRemove(l.fnode, l);
                    linkId = l.tnodeNext;
                }
            }
        }

        return rets;
    }

    @Override
    public boolean[] removeLink(int[] ids)
    {
        Store.L l = new Store.L();

        boolean[] rets = new boolean[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            rets[i] = LSTORE.free(ids[i], l);
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
        Store.N n = new Store.N();

        boolean[] rets = new boolean[nodes.length];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = NSTORE.read(nodes[i].id, n);
            if (!rets[i])
            {
                continue;
            }

            rets[i] = true;

            // remove original props
            removeGObject(n.prop);

            // set new props
            n.prop = putGObject(nodes[i].id, nodes[i]);
            NSTORE.write(nodes[i].id, n);
        }

        return rets;
    }

    @Override
    public boolean[] setLink(Link[] links)
    {
        Store.L l = new Store.L();

        boolean[] rets = new boolean[links.length];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = LSTORE.read(links[i].id, l);
            if (!rets[i])
            {
                continue;
            }

            // remove original props
            removeGObject(l.prop);

            // set new props
            l.prop = putGObject(links[i].id, links[i]);
            LSTORE.write(links[i].id, l);
        }

        return rets;
    }

    @Override
    public Node[] showNodes(int count)
    {
        List<Store.N> nlist = new ArrayList<>(count);
        List<Integer> idList = NSTORE.show(true, count, nlist);

        Node[] nodes = new Node[nlist.size()];
        for (int i = 0; i < nodes.length; ++i)
        {
            nodes[i] = new Node(idList.get(i));
            nodes[i].link = nlist.get(i).link;

            getGObject(nlist.get(i).prop, nodes[i]);
        }

        return nodes;
    }

    @Override
    public Link[] showLinks(int count)
    {
        List<Store.L> llist = new ArrayList<>(count);
        List<Integer> idList = LSTORE.show(false, count, llist);

        Link[] links = new Link[llist.size()];
        for (int i = 0; i < links.length; ++i)
        {
            links[i] = new Link(idList.get(i));
            links[i].fnode = llist.get(i).fnode;
            links[i].tnode = llist.get(i).tnode;

            getGObject(llist.get(i).prop, links[i]);
        }

        return links;
    }

    private int putGObject(int id, GObject<?> obj)
    {
        // category -> ltk.store & prop.store
        int propId = ID_NULL;
        Store.P p = new Store.P();
        for (int i = 0; i < obj.categorys.length; ++i)
        {
            p.fill(FREE_FALSE, ID_NULL, LTKSTORE.write(obj.categorys[i]), propId, id);
            propId = PSTORE.alloc(pebi);
            PSTORE.write(propId, p);
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
                    propValue = VSTORE.write((String)value.value);
                    break;
            }

            p.fill((byte)(FREE_FALSE | value.type), LTKSTORE.write(key), propValue, propId, id);
            propId = PSTORE.alloc(pebi);
            PSTORE.write(propId, p);
        }

        return propId;
    }

    private void getGObject(int propId, GObject<?> obj)
    {
        Store.P p = new Store.P();

        List<String> categoryList = new ArrayList<String>();
        do
        {
            PSTORE.read(propId, p);
            if (p.key == ID_NULL)
            {
                categoryList.add(LTKSTORE.read((int)p.value));
            }
            else
            {
                switch (p.getValueType())
                {
                    case PropValue.TYPE_NUMBERIC:
                        obj.set(LTKSTORE.read(p.key), p.value);
                        break;
                    case PropValue.TYPE_DECIMAL:
                        obj.set(LTKSTORE.read(p.key), Double.longBitsToDouble(p.value));
                        break;
                    case PropValue.TYPE_BOOLEAN:
                        obj.set(LTKSTORE.read(p.key), p.value == 0 ? false : true);
                        break;
                    default:
                        obj.set(LTKSTORE.read(p.key), VSTORE.read(p.value));
                        break;
                }
            }
            propId = p.next;
        } while (p.next != ID_NULL);

        obj.categorys = categoryList.toArray(new String[0]);
    }

    private void removeGObject(int propId)
    {
        Store.P p = new Store.P();
        while (propId != ID_NULL)
        {
            PSTORE.free(propId, p);
            if (p.getValueType() == PropValue.TYPE_STRING)
            {
                VSTORE.free(p.value);
            }
            propId = p.next;
        }
    }

    private int adjustLinkForLinkPut(int nodeId, Store.N n, int linkId)
    {
        // first link
        if (n.link == ID_NULL)
        {
            n.link = linkId;
            NSTORE.write(nodeId, n);
            return ID_NULL;
        }

        // adjust node last link
        int adjustLinkId = n.link;
        Store.L l = new Store.L();
        do
        {
            LSTORE.read(adjustLinkId, l);
            if (l.fnode == nodeId)
            {
                if (l.fnodeNext == ID_NULL)
                {
                    l.fnodeNext = linkId;
                    LSTORE.write(adjustLinkId, l);
                    break;
                }
                else
                {
                    adjustLinkId = l.fnodeNext;
                }
            }
            else
            {
                if (l.tnodeNext == ID_NULL)
                {
                    l.tnodeNext = linkId;
                    LSTORE.write(adjustLinkId, l);
                    break;
                }
                else
                {
                    adjustLinkId = l.tnodeNext;
                }
            }
        } while (true);

        return adjustLinkId;
    }

    private void adjustLinkForLinkRemove(int nodeId, Store.L l)
    {
        boolean isfnode = (nodeId == l.fnode);

        // adjust previous link
        int prev = isfnode ? l.fnodePrev : l.tnodePrev;
        if (prev != ID_NULL)
        {
            Store.L tl = new Store.L();
            LSTORE.read(prev, tl);
            if (tl.fnode == l.fnode)
            {
                tl.fnodeNext = isfnode ? l.fnodeNext : l.tnodeNext;
            }
            else
            {
                tl.tnodeNext = isfnode ? l.fnodeNext : l.tnodeNext;
            }
            LSTORE.write(prev, tl);
        }

        // adjust next link
        int next = isfnode ? l.fnodeNext : l.tnodeNext;
        if (next != ID_NULL)
        {
            Store.L tl = new Store.L();
            LSTORE.read(next, tl);
            if (tl.fnode == l.fnode)
            {
                tl.fnodePrev = isfnode ? l.fnodePrev : l.tnodePrev;
            }
            else
            {
                tl.tnodePrev = isfnode ? l.fnodePrev : l.tnodePrev;
            }
            LSTORE.write(l.fnodeNext, tl);
        }

        // adjust node(remove link is the only one)
        if (prev == ID_NULL)
        {
            Store.N n = new Store.N();
            NSTORE.read(nodeId, n);
            n.link = next;
            NSTORE.write(nodeId, n);
        }
    }

    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();

        System.out.println("escape: " + (System.currentTimeMillis() - start));
    }
}
