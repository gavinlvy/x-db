/*
 * @(#)Config.java, 2018年7月2日 下午5:29:27
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.Loader;

import com.dameng.xdb.util.StringUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class XDB
{
    private final static Logger logger = Logger.getLogger(XDB.class);

    public final static String VERSION = "V2.0";

    public final static int SRC_NUM = 4123;

    public final static String BUILD_TS = "2019.4.14";

    public final static Series SERIES = Series.DEV;

    public final static String ENCODING_UTF_8 = "UTF-8";

    public final static String ENCODING_GB18030 = "GB18030";

    public static final int BYTE_SIZE = 1;

    public static final int INT_SIZE = 4;

    public static final int LONG_SIZE = 8;

    public static void main(String[] args)
    {
        System.out.println(Arrays.toString(XDB.Config.SE_EBI_BITS.value));
    }

    public static enum Series
    {
        DEV("DEV"), PSN("PSN"), ENT("ENT");

        private String name;

        Series(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    @SuppressWarnings ("rawtypes")
    public final static List<Config.ConfigItem> CONFIGS = new ArrayList<>();

    public static class Config
    {
        /*COMMON*/
        public final static ConfigItem<String> ENCODING = new ConfigItem<>("encoding", "utf-8");

        /*SE*/
        public final static ConfigItem<Integer> SE_PORT = new ConfigItem<>("se.port", 3721);

        public final static ConfigItem<Byte[]> SE_EBI_BITS = new ConfigItem<>("se.ebi_bits", new Byte[] {8,
                10, 14}); // total 32bit, to generate gobj_id

        public final static ConfigItem<Integer> SE_MODE = new ConfigItem<>("se.mode", 0);

        public final static ConfigItem<String> SE_RDB_DRIVER = new ConfigItem<>("se.rdb_driver",
                "dm.jdbc.driver.DmDriver");

        public final static ConfigItem<String> SE_RDB_URL = new ConfigItem<>("se.rdb_url",
                "jdbc:dm://localhost:5236");

        public final static ConfigItem<String> SE_RDB_USER = new ConfigItem<>("se.rdb_user", "SYSDBA");

        public final static ConfigItem<String> SE_RDB_PASSWORD = new ConfigItem<>("se.rdb_password", "SYSDBA");

        /*PE*/
        public final static ConfigItem<Integer> PE_PORT = new ConfigItem<>("pe.port", 4728);

        /*ME*/
        public final static ConfigItem<Integer> ME_PORT = new ConfigItem<>("me.port", 5735);

        static
        {
            Properties properties = new Properties();

            try
            {
                properties.load(Loader.getResource("xdb.properties").openStream());
            }
            catch (Exception e)
            {
                logger.error("Config file load fail, default config used!", e);
            }

            properties.forEach(new BiConsumer<Object, Object>()
            {
                @Override
                public void accept(Object k, Object v)
                {
                    String key = StringUtil.trimToEmpty((String)k).toLowerCase();
                    String value = StringUtil.trimToEmpty((String)v);

                    /*COMMON*/
                    if (StringUtil.equalsIgnoreCase(key, ENCODING.name))
                    {
                        ENCODING.value = value;
                    }
                    /*SE*/
                    else if (StringUtil.equalsIgnoreCase(key, SE_PORT.name))
                    {
                        SE_PORT.value = Integer.valueOf(value);
                    }
                    else if (StringUtil.equalsIgnoreCase(key, SE_EBI_BITS.name))
                    {
                        String[] bitStrs = value.split(",");
                        for (int i = 0; i < bitStrs.length; ++i)
                        {
                            SE_EBI_BITS.value[i] = Byte.valueOf(bitStrs[i]);
                        }
                    }
                    else if (StringUtil.equalsIgnoreCase(key, SE_MODE.name))
                    {
                        SE_MODE.value = Integer.valueOf(value);
                    }
                    else if (StringUtil.equalsIgnoreCase(key, SE_RDB_DRIVER.name))
                    {
                        SE_RDB_DRIVER.value = value;
                    }
                    else if (StringUtil.equalsIgnoreCase(key, SE_RDB_URL.name))
                    {
                        SE_RDB_URL.value = value;
                    }
                    else if (StringUtil.equalsIgnoreCase(key, SE_RDB_USER.name))
                    {
                        SE_RDB_USER.value = value;
                    }
                    else if (StringUtil.equalsIgnoreCase(key, SE_RDB_PASSWORD.name))
                    {
                        SE_RDB_PASSWORD.value = value;
                    }
                    /*PE*/
                    else if (StringUtil.equalsIgnoreCase(key, PE_PORT.name))
                    {
                        PE_PORT.value = Integer.valueOf(value);
                    }
                    /*ME*/
                    else if (StringUtil.equalsIgnoreCase(key, ME_PORT.name))
                    {
                        ME_PORT.value = Integer.valueOf(value);
                    }
                }
            });
        }

        public static class ConfigItem<T>
        {
            public String name;

            public T value;

            public ConfigItem(String name, T value)
            {
                this.name = name;
                this.value = value;

                CONFIGS.add(this);
            }

            private String value2String()
            {
                if (value == null)
                {
                    return null;
                }

                if (value.getClass().isArray())
                {
                    return Arrays.toString((Object[])value);
                }

                return value.toString();
            }

            @Override
            public String toString()
            {
                return name + ": " + value2String();
            }
        }
    }
}
