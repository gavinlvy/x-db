/*
 * @(#)XDBTester.java, 2018年10月11日 下午6:42:07
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb;

import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.junit.Test;

import com.dameng.xdb.parser.DescriptiveErrorListener;
import com.dameng.xdb.parser.XDBLexer;
import com.dameng.xdb.parser.XDBParser;
import com.dameng.xdb.se.driver.XDBConnection;
import com.dameng.xdb.se.driver.XDBDriver;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;
import com.dameng.xdb.se.nse.NLPStore;
import com.dameng.xdb.stmt.Statement;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
@SuppressWarnings ("deprecation")
public class XDBTester
{
    public final static String host = "localhost";

    public final static int port = 3721;

    public final static String user = "SYSDBA";

    public final static String password = "SYSDBA";

    public static XDBConnection getConnection()
    {
        return XDBDriver.connect(host, port, user, password);
    }

    public static Node[] getNodes()
    {
        return new Node[] {
                new Node("BOSS", "BOSS2").set("name", "dameng").set("age", 33).set("sex", "m")
                        .set("value", "1.234"),
                new Node("BOSS2").set("name", "dameng").set("age", 33).set("sex", "m").set("value", "1.234")};
    }

    public static Link[] getLinks()
    {
        return new Link[] {new Link("FRIEND").set("cdate", "2013-11-18"),
                new Link("LIKE").set("cdate", "2013-11-18")};
    }

    public static void testSEModel()
    {
        System.out.println(Arrays.toString(getNodes()));
        System.out.println(getLinks()[0]);
    }

    public static void testDriver()
    {
        XDBConnection conn = getConnection();
        conn.close();
    }

    public static void testPut()
    {
        XDBConnection conn = getConnection();
        System.out.println(Arrays.toString(conn.putNodes(getNodes())));
        conn.close();
    }

    public static void testGet(int[] ids)
    {
        XDBConnection conn = getConnection();
        Node[] nodes = conn.getNodes(ids);
        System.out.println(Arrays.toString(nodes));
        conn.close();
    }

    public static void testPutGet()
    {
        Node[] nodes = getNodes();
        System.out.println(Arrays.toString(nodes));

        XDBConnection conn = getConnection();
        int[] ids = conn.putNodes(nodes);
        System.out.println(Arrays.toString(ids));

        nodes = conn.getNodes(ids);
        System.out.println(Arrays.toString(nodes));

    }

    @SuppressWarnings ("unused")
    public static void testNLP()
    {
        long start = System.currentTimeMillis();
        NLPStore store1 = new NLPStore();
        NLPStore store2 = new NLPStore();
        NLPStore store3 = new NLPStore();
        System.out.println("escape: " + (System.currentTimeMillis() - start));
    }

    @Test
    public static void testLexer()
    {
        //        String command = "put node {name= 'Jack', age=33}, {name:'Jack', age::33};";
        String command = "put node('PERSON') {name: 'kevin', male: false}";

        XDBLexer lexer = new XDBLexer(new ANTLRInputStream(command));
        Token token = null;
        while ((token = lexer.nextToken()) != null)
        {
            if (token.getType() == Token.EOF)
            {
                break;
            }
            System.out.println(token);
        }
    }

    @Test
    public static void testParser()
    {
        //        String command = "put node('PERSON') {name: 'Jack', info: '\\'123'};put link(1323, 345, 'FRIEND', 'PARENT') {start: '2010-10-10'};get node(123);get link(123);set link(123) {start: '2010-10-10'};remove node(123);";
        String command = "login 127.0.0.1:3721@sysdba/sysdba;login 127.0.0.1:3728@sysdba/sysdba";

        XDBLexer lexer = new XDBLexer(new ANTLRInputStream(command));
        lexer.removeErrorListeners();
        lexer.addErrorListener(DescriptiveErrorListener.INSTANCE);
        XDBParser parser = new XDBParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(DescriptiveErrorListener.INSTANCE);
        //                 parser.setTrace(true);
        parser.xdb();

        List<Statement> stmtList = parser.stmtList;
        for (Statement stmt : stmtList)
        {
            System.out.println(stmt.getClass().getSimpleName() + ": " + stmt);
        }
    }

    public static void main(String[] args)
    {
        //                        testSEModel();
        //        testDriver();
        //                testPut();
        //        testGet();
        //        testNLP();
        //                testPutGet();
        //        testGet(new int[] {1, 2, 3});
        //        testGet(new int[] {-1677721600, -1677721599});

        testLexer();
        //                testParser();
    }
}
