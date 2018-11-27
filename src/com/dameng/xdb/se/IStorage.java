/*
 * @(#)IStorage.java, 2018年9月10日 下午2:05:11
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se;

import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public interface IStorage
{
    int[] putNodes(Node[] nodes);

    int[] putLinks(Link[] links);

    Node[] getNodes(int[] ids);

    Link[] getLinks(int[] ids);

    boolean[] removeNode(int[] ids);

    boolean[] removeLink(int[] ids);

    boolean[] setNode(Node[] nodes);

    boolean[] setLink(Link[] links);

    Node[] showNodes(int count);

    Link[] showLinks(int count);
}
