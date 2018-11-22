/*
 * @(#)Storage.java, 2018年9月10日 下午2:05:11
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.nse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.IStorage;
import com.dameng.xdb.se.Session;
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
    private final static LTKStore LTK_STORE = new LTKStore();

    private final static PVStore PV_STORE = new PVStore();

    private final static NLPStore NODE_STORE = new NLPStore(XDB.Config.SE_EBI_BITS.value[0],
            XDB.Config.SE_EBI_BITS.value[1], XDB.Config.SE_EBI_BITS.value[2], NLPStore.Node.LENGTH);

    private final static NLPStore LINK_STORE = new NLPStore(XDB.Config.SE_EBI_BITS.value[0],
            XDB.Config.SE_EBI_BITS.value[1], XDB.Config.SE_EBI_BITS.value[2], NLPStore.Link.LENGTH);

    private final static NLPStore PROP_STORE = new NLPStore(XDB.Config.SE_EBI_BITS.value[0],
            XDB.Config.SE_EBI_BITS.value[1], XDB.Config.SE_EBI_BITS.value[2], NLPStore.Prop.LENGTH);

    private final static int ID_NULL = 0;

    private Session session;

    private final int[] NODE_EBI, LINK_EBI, PROP_EBI; // record the session bind extent, and for array cache, used by 'put' only

    public Storage(Session session)
    {
        this.session = session;

        this.NODE_EBI = new int[] {(int)(this.session.id % NODE_STORE.EXTENTS), 0, 0};
        this.LINK_EBI = new int[] {(int)(this.session.id % LINK_STORE.EXTENTS), 0, 0};
        this.PROP_EBI = new int[] {(int)(this.session.id % PROP_STORE.EXTENTS), 0, 0};
    }

    @Override
    public int[] putNodes(Node[] nodes)
    {
        final NLPStore.Node NODE = new NLPStore.Node();

        int[] rets = new int[nodes.length];
        for (int i = 0; i < nodes.length; ++i)
        {
            rets[i] = NODE_STORE.alloc(NODE_EBI);

            NODE.set(NLPStore.FREE_FALSE, putGObject(rets[i], nodes[i]), ID_NULL);
            NODE_STORE.set(rets[i], NODE);
        }

        return rets;
    }

    @Override
    public int[] putLinks(Link[] links)
    {
        final NLPStore.Link LINK = new NLPStore.Link();

        int[] rets = new int[links.length];
        for (int i = 0; i < links.length; ++i)
        {
            final NLPStore.Node FNODE = new NLPStore.Node();
            if (!NODE_STORE.get(links[i].fnode, FNODE))
            {
                XDBException.SE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].fnode));
            }

            final NLPStore.Node TNODE = new NLPStore.Node();
            if (!NODE_STORE.get(links[i].tnode, TNODE))
            {
                XDBException.SE_NODE_NOT_EXISTS.throwException(String.valueOf(links[i].tnode));
            }

            rets[i] = LINK_STORE.alloc(LINK_EBI);
            LINK.set(NLPStore.FREE_FALSE, putGObject(rets[i], links[i]), links[i].fnode, links[i].tnode,
                    adjustLinkForLinkPut(links[i].fnode, FNODE, rets[i]), ID_NULL,
                    adjustLinkForLinkPut(links[i].tnode, TNODE, rets[i]), ID_NULL);

            LINK_STORE.set(rets[i], LINK);
        }

        return rets;
    }

    @Override
    public Node[] getNodes(int[] ids)
    {
        final NLPStore.Node NODE = new NLPStore.Node();

        Node[] nodes = new Node[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            if (!NODE_STORE.get(ids[i], NODE))
            {
                nodes[i] = null;
                continue;
            }

            nodes[i] = new Node(ids[i]);
            nodes[i].link = NODE.link;

            getGObject(NODE.prop, nodes[i]);
        }

        return nodes;
    }

    @Override
    public Link[] getLinks(int[] ids)
    {
        final NLPStore.Link LINK = new NLPStore.Link();

        Link[] links = new Link[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            if (!LINK_STORE.get(ids[i], LINK))
            {
                links[i] = null;
                continue;
            }

            links[i] = new Link(ids[i]);
            links[i].fnode = LINK.fnode;
            links[i].tnode = LINK.tnode;

            getGObject(LINK.prop, links[i]);
        }

        return links;
    }

    @Override
    public boolean[] removeNode(int[] ids)
    {
        final NLPStore.Node NODE = new NLPStore.Node();
        final NLPStore.Link LINK = new NLPStore.Link();

        boolean[] rets = new boolean[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            rets[i] = NODE_STORE.remove(ids[i], NODE);
            if (!rets[i])
            {
                continue;
            }

            // remove node props
            removeGObject(NODE.prop);

            // adjust relate links
            int linkId = NODE.link;
            while (linkId != ID_NULL)
            {
                LINK_STORE.remove(linkId, LINK);
                removeGObject(LINK.prop);
                if (LINK.fnode == ids[i])
                {
                    adjustLinkForLinkRemove(LINK.tnode, LINK);
                    linkId = LINK.fnodeNext;
                }
                else
                {
                    adjustLinkForLinkRemove(LINK.fnode, LINK);
                    linkId = LINK.tnodeNext;
                }
            }
        }

        return rets;
    }

    @Override
    public boolean[] removeLink(int[] ids)
    {
        final NLPStore.Link LINK = new NLPStore.Link();

        boolean[] rets = new boolean[ids.length];
        for (int i = 0; i < ids.length; ++i)
        {
            rets[i] = LINK_STORE.remove(ids[i], LINK);
            if (!rets[i])
            {
                continue;
            }

            // remove link props
            removeGObject(LINK.prop);

            // adjust relate links
            adjustLinkForLinkRemove(LINK.fnode, LINK);
            adjustLinkForLinkRemove(LINK.tnode, LINK);
        }

        return rets;
    }

    @Override
    public boolean[] setNode(Node[] nodes)
    {
        final NLPStore.Node NODE = new NLPStore.Node();

        boolean[] rets = new boolean[nodes.length];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = NODE_STORE.get(nodes[i].id, NODE);
            if (!rets[i])
            {
                continue;
            }

            rets[i] = true;

            // remove original props
            removeGObject(NODE.prop);

            // set new props
            NODE.prop = putGObject(nodes[i].id, nodes[i]);
            NODE_STORE.set(nodes[i].id, NODE);
        }

        return rets;
    }

    @Override
    public boolean[] setLink(Link[] links)
    {
        final NLPStore.Link LINK = new NLPStore.Link();

        boolean[] rets = new boolean[links.length];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = LINK_STORE.get(links[i].id, LINK);
            if (!rets[i])
            {
                continue;
            }

            // remove original props
            removeGObject(LINK.prop);

            // set new props
            LINK.prop = putGObject(links[i].id, links[i]);
            LINK_STORE.set(links[i].id, LINK);
        }

        return rets;
    }

    private int putGObject(int id, GObject<?> obj)
    {
        // category -> ltk.store & prop.store
        int propId = ID_NULL;
        final NLPStore.Prop PROP = new NLPStore.Prop();
        for (int j = 0; j < obj.categorys.length; ++j)
        {
            PROP.set(NLPStore.FREE_FALSE, ID_NULL, LTK_STORE.put(obj.categorys[j]), propId, id);
            propId = PROP_STORE.put(PROP_EBI, PROP);
        }

        // properties -> prop.store
        PropBiConsumer consumer = new PropBiConsumer(propId);
        obj.propMap.forEach(consumer);

        return consumer.propId;
    }

    private void getGObject(int propId, GObject<?> obj)
    {
        final NLPStore.Prop PROP = new NLPStore.Prop();

        List<String> categoryList = new ArrayList<String>();
        do
        {
            PROP_STORE.get(propId, PROP);
            if (PROP.key == ID_NULL)
            {
                categoryList.add(LTK_STORE.getValue((int)PROP.value));
            }
            else
            {
                switch (PROP.getValueType())
                {
                    case PropValue.TYPE_NUMBERIC:
                        obj.set(LTK_STORE.getValue(PROP.key), PROP.value);
                        break;
                    case PropValue.TYPE_DECIMAL:
                        obj.set(LTK_STORE.getValue(PROP.key), Double.longBitsToDouble(PROP.value));
                        break;
                    case PropValue.TYPE_BOOLEAN:
                        obj.set(LTK_STORE.getValue(PROP.key), PROP.value == 0 ? false : true);
                        break;
                    default:
                        obj.set(LTK_STORE.getValue(PROP.key), PV_STORE.get(PROP.value));
                        break;
                }
            }
            propId = PROP.next;
        } while (PROP.next != ID_NULL);

        obj.categorys = categoryList.toArray(new String[0]);
    }

    private void removeGObject(int propId)
    {
        final NLPStore.Prop PROP = new NLPStore.Prop();
        while (propId != ID_NULL)
        {
            PROP_STORE.remove(propId, PROP);
            if (PROP.getValueType() == PropValue.TYPE_STRING)
            {
                PV_STORE.remove(PROP.value);
            }
            propId = PROP.next;
        }
    }

    private int adjustLinkForLinkPut(int nodeId, final NLPStore.Node NODE, int linkId)
    {
        // first link
        if (NODE.link == ID_NULL)
        {
            NODE.link = linkId;
            NODE_STORE.set(nodeId, NODE);
            return ID_NULL;
        }

        // adjust node last link
        int adjustLinkId = NODE.link;
        final NLPStore.Link LINK = new NLPStore.Link();
        do
        {
            LINK_STORE.get(adjustLinkId, LINK);
            if (LINK.fnode == nodeId)
            {
                if (LINK.fnodeNext == ID_NULL)
                {
                    LINK.fnodeNext = linkId;
                    LINK_STORE.set(adjustLinkId, LINK);
                    break;
                }
                else
                {
                    adjustLinkId = LINK.fnodeNext;
                }
            }
            else
            {
                if (LINK.tnodeNext == ID_NULL)
                {
                    LINK.tnodeNext = linkId;
                    LINK_STORE.set(adjustLinkId, LINK);
                    break;
                }
                else
                {
                    adjustLinkId = LINK.tnodeNext;
                }
            }
        } while (true);

        return adjustLinkId;
    }

    private void adjustLinkForLinkRemove(int nodeId, final NLPStore.Link LINK)
    {
        boolean isfnode = (nodeId == LINK.fnode);

        // adjust previous link
        int prev = isfnode ? LINK.fnodePrev : LINK.tnodePrev;
        if (prev != ID_NULL)
        {
            NLPStore.Link link = new NLPStore.Link();
            LINK_STORE.get(prev, link);
            if (link.fnode == LINK.fnode)
            {
                link.fnodeNext = isfnode ? LINK.fnodeNext : LINK.tnodeNext;
            }
            else
            {
                link.tnodeNext = isfnode ? LINK.fnodeNext : LINK.tnodeNext;
            }
            LINK_STORE.set(prev, link);
        }

        // adjust next link
        int next = isfnode ? LINK.fnodeNext : LINK.tnodeNext;
        if (next != ID_NULL)
        {
            NLPStore.Link link = new NLPStore.Link();
            LINK_STORE.get(next, link);
            if (link.fnode == LINK.fnode)
            {
                link.fnodePrev = isfnode ? LINK.fnodePrev : LINK.tnodePrev;
            }
            else
            {
                link.tnodePrev = isfnode ? LINK.fnodePrev : LINK.tnodePrev;
            }
            LINK_STORE.set(LINK.fnodeNext, link);
        }

        // adjust node(remove link is the only one)
        if (prev == ID_NULL)
        {
            final NLPStore.Node NODE = new NLPStore.Node();
            NODE_STORE.get(nodeId, NODE);
            NODE.link = next;
            NODE_STORE.set(nodeId, NODE);
        }
    }

    class PropBiConsumer implements BiConsumer<String, PropValue>
    {
        public int propId = ID_NULL;

        private final NLPStore.Prop PROP = new NLPStore.Prop();

        public PropBiConsumer(int propId)
        {
            this.propId = propId;
        }

        @Override
        public void accept(String key, PropValue value)
        {
            // prop_value
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
                    propValue = PV_STORE.put((String)value.value);
                    break;
            }

            PROP.set((byte)(NLPStore.FREE_FALSE | value.type), LTK_STORE.put(key), propValue, propId,
                    ID_NULL);

            propId = PROP_STORE.put(PROP_EBI, PROP);
        }
    }

    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();

        System.out.println("escape: " + (System.currentTimeMillis() - start));
    }
}
