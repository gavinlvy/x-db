/*
 * @(#)GObject.java, 2018年10月19日 上午11:30:51
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.model;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
@SuppressWarnings ("unchecked")
public class GObject<T>
{
    public int id;

    public String[] categorys;

    public Map<String, PropValue> propMap = new HashMap<>();

    public GObject(int id)
    {
        this.id = id;
        this.categorys = new String[0];
    }

    public GObject(String... categorys)
    {
        this.categorys = categorys == null ? new String[0] : categorys;
    }

    public GObject(int id, String... categorys)
    {
        this.id = id;
        this.categorys = categorys == null ? new String[0] : categorys;
    }

    public T set(String key, long value)
    {
        propMap.put(key, new PropValue(value, PropValue.TYPE_NUMBERIC));
        return (T)this;
    }

    public T set(String key, double value)
    {
        propMap.put(key, new PropValue(value, PropValue.TYPE_DECIMAL));
        return (T)this;
    }

    public T set(String key, boolean value)
    {
        propMap.put(key, new PropValue(value, PropValue.TYPE_BOOLEAN));
        return (T)this;
    }

    public T set(String key, String value)
    {
        propMap.put(key, new PropValue(value != null ? value : "", PropValue.TYPE_STRING));
        return (T)this;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append(id);
        sb.append(" - [");
        
        if (categorys != null && categorys.length > 0)
        {
            for (String cat : categorys)
            {
                sb.append(cat);
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
        }

        sb.append("] - {");

        if (propMap != null && propMap.size() > 0)
        {
            propMap.forEach(new BiConsumer<String, PropValue>()
            {
                @Override
                public void accept(String t, PropValue u)
                {
                    sb.append(t);
                    sb.append(": ");
                    sb.append(u);
                    sb.append(", ");
                }
            });
            sb.delete(sb.length() - 2, sb.length());
        }

        sb.append("}");

        return sb.toString();
    }
}
