/*
 * @(#)XSE.java, 2018年9月7日 下午2:25:31
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.dameng.xdb.XDB;
import com.dameng.xdb.util.StringUtil;

/**
 * storage site
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class StorageEngine
{
    private final static Logger logger = Logger.getLogger(StorageEngine.class);

    private static String[] checkArgs(String[] args)
    {
        args = new String[] {"xdb.properties"};
        if (args == null || args.length == 0)
        {
            logger.error("Invalid arguments, usage as: xs [-noconsole] xdb.properties");
            System.exit(0);
        }
        return args;
    }

    public static void main(String[] args)
    {
        args = checkArgs(args);

        logger.info("XStore " + XDB.VERSION + "." + XDB.SRC_NUM + "-" + XDB.BUILD_TS + "-" + XDB.SERIES
                + " starting...");

        ServerSocket serverSocket = null;
        try
        {
            serverSocket = new ServerSocket(XDB.Config.SE_PORT.value);
        }
        catch (IOException e)
        {
            logger.error("Create socket fail!", e);
            System.exit(0);
        }

        new Listener(serverSocket).start();

        logger.info("XStore is ready.");

        // console
        if (StringUtil.equalsIgnoreCase(args[0], "-noconsole"))
        {
            return;
        }

        Scanner scanner = new Scanner(System.in);
        exit: while (true)
        {
            switch (StringUtil.trimToEmpty(scanner.next()).toLowerCase())
            {
                case "exit":
                    System.exit(0);
                    break exit;
                case "session":
                    System.out.println("sessions: " + Session.COUNTER.get());
                    break;
                case "config":
                    System.out.println(XDB.config());
                    break;
                default:
                    System.out.println("Invalid command!");
                    break;
            }
        }
        scanner.close();
    }
}
