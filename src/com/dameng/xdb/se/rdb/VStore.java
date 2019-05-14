/*
 * @(#)VStore.java, 2019年3月14日 下午8:07:16
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
public class VStore extends Store
{
    private PreparedStatement putStmt;

    private PreparedStatement getStmt;

    public VStore(Connection connection) throws SQLException
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

    public boolean read(int id, V v)
    {
        // TODO
        return false;
    }

    public long write(V v) throws SQLException
    {
        long id = IStorage.ID_NULL;

        this.putStmt.setString(1, v.value);
        this.putStmt.executeUpdate();
        ResultSet rs = this.putStmt.getGeneratedKeys();
        if (rs.next())
        {
            id = rs.getLong(1);
        }
        rs.close();

        return id;
    }
}
