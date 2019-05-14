/*
 * @(#)LStore.java, 2019年3月14日 下午8:46:43
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
import com.dameng.xdb.se.rdb.Store;
import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class LStore extends Store
{
    private PreparedStatement putStmt;

    private PreparedStatement getStmt;

    public LStore(Connection connection) throws SQLException
    {
        this.putStmt = connection
                .prepareStatement(
                        "insert into rdb_link(info, prop, fnode, tnode, fnode_prev, fnode_next, tnode_prev, tnode_next) values(?, ?, ?, ?, ?, ?, ?, ?);",
                        PreparedStatement.RETURN_GENERATED_KEYS);
        this.getStmt = connection
                .prepareStatement("select info, prop, fnode, tnode, fnode_prev, fnode_next, tnode_prev, tnode_next from rdb_link where rowid = ?");
    }

    @Override
    public void destory()
    {
        super.destory();

        MiscUtil.close(this.putStmt);
        MiscUtil.close(this.getStmt);
    }

    public boolean read(int id, L l)
    {
        // TODO
        return false;
    }
    
    public int write(L l) throws SQLException
    {
        int id = IStorage.ID_NULL;

        this.putStmt.setByte(1, l.info);
        this.putStmt.setInt(2, l.prop);
        this.putStmt.setInt(3, l.fnode);
        this.putStmt.setInt(4, l.tnode);
        this.putStmt.setInt(5, l.fnode_prev);
        this.putStmt.setInt(6, l.fnode_next);
        this.putStmt.setInt(7, l.tnode_prev);
        this.putStmt.setInt(8, l.tnode_next);
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
