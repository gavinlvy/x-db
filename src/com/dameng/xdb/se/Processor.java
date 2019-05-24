/*
 * @(#)Processor.java, 2018年9月10日 上午10:17:11
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.driver.msg.DUMMY;
import com.dameng.xdb.se.driver.msg.GET;
import com.dameng.xdb.se.driver.msg.LOGIN;
import com.dameng.xdb.se.driver.msg.MSG;
import com.dameng.xdb.se.driver.msg.PUT;
import com.dameng.xdb.se.driver.msg.REMOVE;
import com.dameng.xdb.se.driver.msg.SET;
import com.dameng.xdb.se.driver.msg.SHOW;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;
import com.dameng.xdb.util.MiscUtil;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Processor extends Thread
{
    private final static Logger LOGGER = Logger.getLogger(Processor.class);

    private Socket socket;

    private InputStream is;

    private OutputStream os;

    private Buffer buffer;

    private MSG msg;

    public Session session;

    public IStorage storage;

    public Processor(Socket socket)
    {
        this.socket = socket;
    }

    public void initialize() throws Exception
    {
        this.is = new BufferedInputStream(this.socket.getInputStream(), MSG.NET_PACKET_SIZE);
        this.os = new BufferedOutputStream(this.socket.getOutputStream(), MSG.NET_PACKET_SIZE);

        this.buffer = Buffer.allocateBytes(MSG.NET_PACKET_SIZE);

        this.session = new Session();

        this.storage = XDB.Config.SE_MODE.value == 1 ? new com.dameng.xdb.se.rdb.Storage(this)
                : new com.dameng.xdb.se.nse.Storage(this);
        this.storage.initialize();
    }

    public void destory()
    {
        MiscUtil.close(this.is);
        MiscUtil.close(this.os);
        MiscUtil.close(this.socket);

        this.buffer = null;

        this.storage.destory();
    }

    @Override
    public void run()
    {
        LOGGER.info("Processor start for [" + socket.getRemoteSocketAddress() + "]...");

        try
        {
            initialize();

            while (true)
            {
                // receive
                this.buffer.clear();
                this.buffer.load(this.is, MSG.HEAD_LENGTH);
                int length = this.buffer.getInt(MSG.OFFSET_LENGTH);
                if (length > 0)
                {
                    this.buffer.load(this.is, length);
                }

                // process
                this.msg = createMsg(this.buffer.getByte(MSG.OFFSET_COMMAND));
                this.msg.decode();
                try
                {
                    process();
                }
                catch (Throwable t)
                {
                    LOGGER.error("Processor error!", t);
                    this.msg.exception = t instanceof XDBException ? (XDBException)t
                            : XDBException.SE_PROCESS_ERROR;
                }
                finally
                {
                    this.msg.encode();
                }

                // send
                this.buffer.flip();
                this.buffer.flush(this.os);
            }
        }
        catch (Throwable t)
        {
            LOGGER.error("Process exception!", t);
        }
        finally
        {
            LOGGER.info("Processor over for [" + this.socket.getRemoteSocketAddress() + "].");

            destory();
        }
    }

    private MSG createMsg(byte command)
    {
        MSG msg = null;

        switch (command)
        {
            case MSG.COMMAND_CONNECT:
                msg = new LOGIN.S(this.buffer);
                break;
            case MSG.COMMAND_PUT:
                msg = new PUT.S(this.buffer, this.session.encoding);
                break;
            case MSG.COMMAND_GET:
                msg = new GET.S(this.buffer, this.session.encoding);
                break;
            case MSG.COMMAND_SET:
                msg = new SET.S(this.buffer, this.session.encoding);
                break;
            case MSG.COMMAND_REMOVE:
                msg = new REMOVE.S(this.buffer, this.session.encoding);
                break;
            case MSG.COMMAND_SHOW:
                msg = new SHOW.S(this.buffer, this.session.encoding);
                break;
            default:
                msg = new DUMMY.S(this.buffer, this.session.encoding);
                break;
        }

        return msg;
    }

    private void process()
    {
        switch (this.buffer.getByte(MSG.OFFSET_COMMAND))
        {
            case MSG.COMMAND_CONNECT:
                connect();
                break;
            case MSG.COMMAND_PUT:
                put();
                break;
            case MSG.COMMAND_GET:
                get();
                break;
            case MSG.COMMAND_SET:
                set();
                break;
            case MSG.COMMAND_REMOVE:
                remove();
                break;
            case MSG.COMMAND_SHOW:
                show();
                break;
            default:
                dummy();
                break;
        }
    }

    private void connect()
    {
        LOGIN msg = (LOGIN)this.msg;

        msg.encoding = XDB.Config.ENCODING.value;
    }

    private void put()
    {
        PUT msg = (PUT)this.msg;

        msg.ids = msg.node ? storage.putNodes((Node[])msg.objs) : storage.putLinks((Link[])msg.objs);
    }

    private void get()
    {
        GET msg = (GET)this.msg;

        msg.objs = msg.node ? storage.getNodes(msg.ids) : storage.getLinks(msg.ids);
    }

    private void set()
    {
        SET msg = (SET)this.msg;
        msg.rets = msg.node ? storage.setNode((Node[])msg.objs) : storage.setLink((Link[])msg.objs);
    }

    private void remove()
    {
        REMOVE.S msg = (REMOVE.S)this.msg;

        msg.rets = msg.node ? storage.removeNode(msg.ids) : storage.removeLink(msg.ids);
    }

    private void show()
    {
        SHOW.S msg = (SHOW.S)this.msg;
        msg.objs = msg.node ? storage.showNodes(msg.count) : storage.showLinks(msg.count);
    }

    private void dummy()
    {
        this.msg.exception = XDBException.SE_MSG_COMMAND_INVALID;
    }
}
