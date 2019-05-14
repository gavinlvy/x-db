/*
 * @(#)PStore.java, 2019年3月14日 下午7:46:58
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
public class PStore extends Store
{
    private PreparedStatement putStmt;

    private PreparedStatement getStmt;

    public PStore(Connection connection) throws SQLException
    {
        this.putStmt = connection
                .prepareStatement("insert into rdb_prop(info, key, value, next) values(?, ?, ?, ?);");
        this.getStmt = connection
                .prepareStatement("select info, key, value, next from rdb_prop where rowid=?;");
    }

    @Override
    public void destory()
    {
        super.destory();

        MiscUtil.close(this.putStmt);
        MiscUtil.close(this.getStmt);
    }

    public boolean read(int id, P p)
    {
        // TODO
        return false;
    }
    
    public int write(P p) throws SQLException
    {
        int id = IStorage.ID_NULL;

        this.putStmt.setByte(1, p.info);
        this.putStmt.setInt(2, p.key);
        this.putStmt.setLong(3, p.value);
        this.putStmt.setInt(4, p.next);
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
