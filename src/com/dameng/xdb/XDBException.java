/*
 * @(#)XDBException.java, 2018年11月1日 下午2:09:19
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb;

import java.text.MessageFormat;

import com.dameng.xdb.i18n.Messages;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
@SuppressWarnings ("serial")
public class XDBException extends RuntimeException
{
    // COMMON
    public final static XDBException INTERNAL_ERROR = new XDBException(0000, Messages.get("internal_error"));

    // SE
    public final static XDBException SE_COMMUNICATE_ERROR = new XDBException(1000,
            Messages.get("se.communicate_error"));

    public final static XDBException SE_PROCESS_ERROR = new XDBException(1001,
            Messages.get("se.process_error"));

    public final static XDBException SE_MSG_CHECK_ERROR = new XDBException(1002,
            Messages.get("se.msg_check_error"));

    public final static XDBException SE_MSG_COMMAND_INVALID = new XDBException(1003,
            Messages.get("se.msg_command_invalid"));

    public final static XDBException SE_UNSUPPORT = new XDBException(1004, Messages.get("se.unsupport"));

    public final static XDBException SE_PUT_FAIL = new XDBException(1005, Messages.get("se.put_fail"));

    public final static XDBException SE_GET_FAIL = new XDBException(1006, Messages.get("se.get_fail"));

    public final static XDBException SE_NO_MORE_SPACE = new XDBException(1007,
            Messages.get("se.no_more_space"));
    
    public final static XDBException SE_INVALID_PARAM = new XDBException(1008, Messages.get("se.invalid_param"));

    public final static XDBException SE_SYNTEX_ERROR = new XDBException(1009, Messages.get("se.syntex_error"));

    public final static XDBException SE_NSE_NODE_NOT_EXISTS = new XDBException(1010, Messages.get("se.node_not_exists"));

    public final static XDBException SE_RDB_ACCESS_ERROR = new XDBException(1200, Messages.get("se.rdb_access_error"));
    
    // PE
    public final static XDBException PE_COMMUNICATE_ERROR = new XDBException(1000,
            Messages.get("pe.communicate_error"));

    public int errCode;

    public String desc;

    private XDBException(int errCode, String desc)
    {
        super("[" + errCode + "] " + desc);
        this.errCode = errCode;
        this.desc = desc;
    }

    private XDBException(int errCode, String desc, Throwable cause)
    {
        super("[" + errCode + "] " + desc, cause);
        this.errCode = errCode;
        this.desc = desc;
    }

    public void throwException(String... args)
    {
        throw new XDBException(errCode,
                (args != null && args.length > 0) ? new MessageFormat(desc).format(args) : desc);
    }

    public void throwException(Throwable t, String... args)
    {
        throw new XDBException(errCode,
                (args != null && args.length > 0) ? new MessageFormat(desc).format(args) : desc, t);
    }

    public static void throwException(int errCode, String desc)
    {
        throw new XDBException(errCode, desc);
    }

    public static void throwException(int errCode, String desc, Throwable cause)
    {
        throw new XDBException(errCode, desc, cause);
    }
}
