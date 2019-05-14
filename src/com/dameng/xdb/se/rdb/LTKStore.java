/*
 * @(#)LTKStore.java, 2019年3月14日 下午7:07:03
 *
 * Copyright (c) 2000-2019, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.rdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.dameng.xdb.se.IStorage;
import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class LTKStore extends Store
{
    private PreparedStatement putStmt;

    private PreparedStatement getStmt;

    public LTKStore(Connection connection) throws SQLException
    {
        this.putStmt = connection.prepareStatement("insert into rdb_v(value) values(?);");
        this.getStmt = connection.prepareStatement("select value from rdb_v where rowid = ?;");
    }

    @Override
    public void destory()
    {
        super.destory();

        MiscUtil.close(this.putStmt);
        MiscUtil.close(this.getStmt);
    }

    public int put(String value) throws SQLException
    {
        // TODO
        int id = IStorage.ID_NULL;

        this.putStmt.setString(1, value);
        this.putStmt.executeUpdate();
        ResultSet rs = this.putStmt.getGeneratedKeys();
        if (rs.next())
        {
            id = rs.getInt(1);
        }
        rs.close();

        return id;
    }
}
