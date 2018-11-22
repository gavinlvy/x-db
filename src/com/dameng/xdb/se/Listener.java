/*
 * @(#)Listener.java, 2018年9月10日 上午10:16:17
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se;

import java.net.ServerSocket;

import org.apache.log4j.Logger;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class Listener extends Thread
{
    private Logger logger = Logger.getLogger(getClass());

    private ServerSocket serverSocket;

    public Listener(ServerSocket serverSocket)
    {
        this.serverSocket = serverSocket;
    }

    @Override
    public void run()
    {
        while (true)
        {
            try
            {
                new Processor(serverSocket.accept()).start();
            }
            catch (Throwable e)
            {
                logger.warn("Lintener accept fail!", e);
            }
        }
    }
}
