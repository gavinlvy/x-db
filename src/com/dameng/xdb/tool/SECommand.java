/*
 * @(#)SEComand.java, 2018年11月6日 上午10:00:53
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import com.dameng.xdb.XDB;
import com.dameng.xdb.XDBException;
import com.dameng.xdb.parser.DescriptiveErrorListener;
import com.dameng.xdb.parser.XDBLexer;
import com.dameng.xdb.parser.XDBParser;
import com.dameng.xdb.se.driver.XDBConnection;
import com.dameng.xdb.se.driver.XDBDriver;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;
import com.dameng.xdb.stmt.SEExit;
import com.dameng.xdb.stmt.SEGet;
import com.dameng.xdb.stmt.SEHelp;
import com.dameng.xdb.stmt.SELogin;
import com.dameng.xdb.stmt.SELogout;
import com.dameng.xdb.stmt.SEPut;
import com.dameng.xdb.stmt.SERemove;
import com.dameng.xdb.stmt.SESet;
import com.dameng.xdb.stmt.SEShow;
import com.dameng.xdb.stmt.Statement;
import com.dameng.xdb.util.MiscUtil;
import com.dameng.xdb.util.StringUtil;

import jline.console.ConsoleReader;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
@SuppressWarnings ("deprecation")
public class SECommand
{
    private XDBConnection conn;

    private void start() throws IOException
    {
        System.out.println("SECommand " + XDB.VERSION + "." + XDB.SRC_NUM + "-" + XDB.BUILD_TS + "-"
                + XDB.SERIES + " starting...");

        ConsoleReader reader = new ConsoleReader();

        exit: while (true)
        {
            // read
            List<Statement> stmtList = null;
            try
            {
                stmtList = parse(
                        StringUtil.trimToEmpty(reader.readLine(conn != null ? "XDB=SE> " : "XDB-SE> ")));
            }
            catch (XDBException e)
            {
                System.out.println(e.getMessage());
                continue;
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                continue;
            }

            // execute
            int i = 0;
            for (Statement stmt : stmtList)
            {
                i++;
                long start = System.currentTimeMillis();
                try
                {
                    switch (stmt.type)
                    {
                        case Statement.TYPE_SE_LOGIN:
                            login((SELogin)stmt);
                            break;
                        case Statement.TYPE_SE_LOGOUT:
                            logout((SELogout)stmt);
                            break;
                        case Statement.TYPE_SE_PUT:
                            put((SEPut)stmt);
                            break;
                        case Statement.TYPE_SE_GET:
                            get((SEGet)stmt);
                            break;
                        case Statement.TYPE_SE_REMOVE:
                            remove((SERemove)stmt);
                            break;
                        case Statement.TYPE_SE_SET:
                            set((SESet)stmt);
                            break;
                        case Statement.TYPE_SE_SHOW:
                            show((SEShow)stmt);
                            break;
                        case Statement.TYPE_SE_EXIT:
                            exit((SEExit)stmt);
                            break exit;
                        case Statement.TYPE_SE_HELP:
                            help((SEHelp)stmt);
                            break;
                        default:
                            break;
                    }
                }
                catch (XDBException e)
                {
                    System.out.println(e.getMessage());
                    if (e.errCode == XDBException.SE_COMMUNICATE_ERROR.errCode)
                    {
                        closeConnetion();
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                }

                System.out.println("Tips: statement " + i + " executed, escape "
                        + (System.currentTimeMillis() - start) + "ms.");
                System.out.println();
            }
        }
        reader.close();
    }

    private List<Statement> parse(String command)
    {
        if (StringUtil.isEmpty(command))
        {
            return new ArrayList<Statement>();
        }

        XDBLexer lexer = new XDBLexer(new ANTLRInputStream(command));
        lexer.removeErrorListeners();
        lexer.addErrorListener(DescriptiveErrorListener.INSTANCE);
        XDBParser parser = new XDBParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(DescriptiveErrorListener.INSTANCE);
        //                 parser.setTrace(true);
        parser.setBuildParseTree(false);
        parser.xdb();
        return parser.stmtList;
    }

    private void closeConnetion()
    {
        if (conn == null)
        {
            return;
        }

        MiscUtil.close(conn);
        conn = null;
    }

    private boolean checkLogin()
    {
        if (conn == null)
        {
            System.out.println("login first.");
        }

        return conn != null;
    }

    private void login(SELogin login)
    {
        closeConnetion();

        try
        {
            conn = XDBDriver.connect(login.host, login.port, login.user, login.password);
            System.out.println("login success.");
        }
        catch (Exception e)
        {
            System.out.println("login fail: " + e.getMessage());
        }
    }

    private void logout(SELogout logout)
    {
        closeConnetion();

        System.out.println("logout success.");
    }

    private void put(SEPut put)
    {
        if (!checkLogin())
        {
            return;
        }

        if (put.node)
        {
            put.obj.id = conn.putNodes((Node)put.obj)[0];
        }
        else
        {
            put.obj.id = conn.putLinks((Link)put.obj)[0];
        }

        System.out.println(put.obj);
    }

    private void get(SEGet get)
    {
        if (!checkLogin())
        {
            return;
        }

        GObject<?>[] objs = null;
        if (get.node)
        {
            objs = conn.getNodes(get.ids);
        }
        else
        {
            objs = conn.getLinks(get.ids);
        }

        for (Object obj : objs)
        {
            System.out.println(obj);
        }
    }

    private void remove(SERemove remove)
    {
        if (!checkLogin())
        {
            return;
        }

        boolean[] rets = null;
        if (remove.node)
        {
            rets = conn.removeNodes(remove.ids);
        }
        else
        {
            rets = conn.removeLinks(remove.ids);
        }

        for (boolean ret : rets)
        {
            System.out.println(ret);
        }
    }

    private void set(SESet set)
    {
        if (!checkLogin())
        {
            return;
        }

        boolean[] rets = null;
        if (set.node)
        {
            rets = conn.setNodes((Node)set.obj);
        }
        else
        {
            rets = conn.setLinks((Link)set.obj);
        }

        for (boolean ret : rets)
        {
            System.out.println(ret);
        }
    }

    private void show(SEShow show)
    {
        if (!checkLogin())
        {
            return;
        }

        GObject<?>[] objs = null;
        if (show.node)
        {
            objs = conn.showNodes(show.count);
        }
        else
        {
            objs = conn.showLinks(show.count);
        }

        for (Object obj : objs)
        {
            System.out.println(obj);
        }
    }

    private void exit(SEExit exit)
    {
        System.exit(0);
    }
    
    private void help(SEHelp help)
    {
        StringBuilder helpInfo = new StringBuilder();
        helpInfo.append("\nUsage:");
        helpInfo.append("\n----------------------------------------------------------");
        helpInfo.append("\n- login HOST:PORT                                        -");
        helpInfo.append("\n- put node(CATEGORYS) { PROPERTIES }                     -");
        helpInfo.append("\n- put link(FNODE_ID, TNODE_ID, CATEGORYS) { PROPERTIES } -");
        helpInfo.append("\n- get node(IDS)                                          -");
        helpInfo.append("\n- get link(IDS)                                          -");
        helpInfo.append("\n- set node(ID, CATEGORYS) { PROPERTIES }                 -");
        helpInfo.append("\n- set link(ID, CATEGORYS) { PROPERTIES }                 -");
        helpInfo.append("\n- remove node(IDS)                                       -");
        helpInfo.append("\n- remove link(IDS)                                       -");
        helpInfo.append("\n- show node top NUMBER                                   -");
        helpInfo.append("\n- show link top NUMBER                                   -");
        helpInfo.append("\n- logout                                                 -");
        helpInfo.append("\n- help                                                   -");
        helpInfo.append("\n- exit                                                   -");
        helpInfo.append("\n----------------------------------------------------------\n");
        System.out.println(helpInfo);
    }

    public static void main(String[] args) throws IOException
    {
        SECommand command = new SECommand();
        command.start();
    }
}
