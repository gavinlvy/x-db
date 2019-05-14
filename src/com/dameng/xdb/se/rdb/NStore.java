/*
 * @(#)NStore.java, 2019年3月14日 下午8:44:42
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
public class NStore extends Store
{
    private PreparedStatement putStmt;
    
    private PreparedStatement getStmt;
    
    public NStore(Connection connection) throws SQLException
    {
        this.putStmt = connection.prepareStatement("insert into rdb_node(info, prop, link) values(?, ?, ?);");
        this.getStmt = connection.prepareStatement("select info, prop, link where id = ?;");
    }
    
    @Override
    public void destory()
    {
        super.destory();
        
        MiscUtil.close(this.putStmt);
        MiscUtil.close(this.getStmt);
    }

    public boolean read(int id, N n)
    {
        // TODO
        return false;
    }
    
    public int write(N n) throws SQLException
    {
        int id = IStorage.ID_NULL;

        this.putStmt.setByte(1, n.info);
        this.putStmt.setInt(2, n.prop);
        this.putStmt.setInt(3, n.link);
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
