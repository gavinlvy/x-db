package com.dameng.xdb.i18n;

import java.util.ResourceBundle;

import com.dameng.xdb.util.StringUtil;

public class Messages
{
    private final static String RESOURCE_NAME = "com.dameng.xdb.i18n.messages";

    // bundle 加载次序： 指定 -》 默认-》无后缀，英文支持需要language & country 为 ""
    //    private final static ResourceBundle resourceBundle = (DmSvcConf.getLanguage() == Const.LANGUAGE_CN)
    //            ? ResourceBundle.getBundle(RESOURCE_NAME, Locale.CHINA)
    //            : ResourceBundle.getBundle(RESOURCE_NAME, new Locale("", ""));

    private final static ResourceBundle resourceBundle = ResourceBundle.getBundle(RESOURCE_NAME);

    public static String get(String key)
    {
        if (StringUtil.isEmpty(key))
        {
            return StringUtil.EMPTY;
        }

        return resourceBundle.getString(key);
    }

    public static void main(String[] args)
    {
        System.out.println(resourceBundle.getString("se.internal_error"));
    }
}
