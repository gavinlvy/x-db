/*
 * @(#)XDBAccess.java, 2018年9月21日 下午1:34:02
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.driver.msg.LOGIN;
import com.dameng.xdb.se.driver.msg.GET;
import com.dameng.xdb.se.driver.msg.MSG;
import com.dameng.xdb.se.driver.msg.PUT;
import com.dameng.xdb.se.driver.msg.REMOVE;
import com.dameng.xdb.se.driver.msg.SET;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.util.MiscUtil;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class XDBAccess
{
    public Socket socket;

    public InputStream is;

    public OutputStream os;

    public Buffer buffer;

    public XDBConnection connection;

    public XDBAccess(XDBConnection connection)
    {
        this.connection = connection;

        this.buffer = Buffer.allocateBytes(MSG.NET_PACKET_SIZE);

        try
        {
            this.socket = new Socket(connection.host, connection.port);

            this.is = new BufferedInputStream(this.socket.getInputStream(), MSG.NET_PACKET_SIZE);
            this.os = new BufferedOutputStream(socket.getOutputStream(), MSG.NET_PACKET_SIZE);
        }
        catch (IOException e)
        {
            close();
            XDBException.SE_COMMUNICATE_ERROR.throwException(e);
        }
    }

    public void close()
    {
        MiscUtil.close(is);
        MiscUtil.close(os);
        MiscUtil.close(socket);
    }

    public void login()
    {
        LOGIN msg = new LOGIN.C(buffer);
        access(msg);
        connection.encoding = msg.encoding;
    }

    public int[] put(boolean node, GObject<?>... objs)
    {
        PUT msg = new PUT.C(this, node, objs);
        access(msg);
        return msg.ids;
    }

    public GObject<?>[] get(boolean node, int... ids)
    {
        GET msg = new GET.C(this, node, ids);
        access(msg);
        return msg.objs;
    }
    
    public boolean[] remove(boolean node, int... ids)
    {
        REMOVE msg = new REMOVE.C(this, node, ids);
        access(msg);
        return msg.rets;
    }

    public boolean[] set(boolean node, GObject<?>... objs)
    {
        SET msg = new SET.C(this, node, objs);
        access(msg);
        return msg.rets;
    }
    
    private void access(MSG msg)
    {
        try
        {
            // send
            msg.encode();

            buffer.flip();
            buffer.flush(os);

            // receive
            buffer.clear();
            buffer.load(is, MSG.HEAD_LENGTH);
            int length = buffer.getInt(MSG.OFFSET_LENGTH);
            if (length > 0)
            {
                buffer.load(is, length);
            }

            msg.decode();
        }
        catch (IOException e)
        {
            close();
            XDBException.SE_COMMUNICATE_ERROR.throwException(e);
        }
    }
}
