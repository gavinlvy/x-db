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

import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class VStore extends Store
{
    public VStore(Connection connection)
    {
        super(connection);
    }

    @Override
    public void initialize() throws Exception
    {
        super.initialize();

        this.getStmt = this.connection.prepareStatement("select id, value from rdb_v where id = ?;");
        this.putStmt = this.connection.prepareStatement("insert into rdb_v(id, value) values(?, ?);");
        this.setStmt = this.connection.prepareStatement("update rdb_v set value = ? where id = ?;");
        this.removeStmt = this.connection.prepareStatement("delete from rdb_v where id = ?;");
        this.showStmt = this.connection.prepareStatement("select top ? id, value from rdb_v;");
    }

    @Override
    public void destory() throws Exception
    {
        super.destory();

        MiscUtil.close(this.getStmt);
        MiscUtil.close(this.putStmt);
        MiscUtil.close(this.setStmt);
        MiscUtil.close(this.removeStmt);
        MiscUtil.close(this.showStmt);
    }

    public static class V extends Item
    {
        public String value;

        public V fill(String value)
        {
            this.value = value;
            return this;
        }

        @Override
        public Item encode(PreparedStatement pstmt, int type) throws SQLException
        {
            if (type == Item.ENCODE_TYPE_READ)
            {
                pstmt.setInt(1, this.id);
            }
            else if (type == Item.ENCODE_TYPE_WRITE)
            {
                pstmt.setInt(1, this.id);
                pstmt.setString(2, this.value);
            }
            else if (type == Item.ENCODE_TYPE_UPDATE)
            {
                pstmt.setString(1, this.value);
                pstmt.setInt(2, this.id);
            }
            else if (type == Item.ENCODE_TYPE_REMOVE)
            {
                pstmt.setInt(1, this.id);
            }
            else if (type == Item.ENCODE_TYPE_SHOW)
            {
                pstmt.setInt(1, this.id);
            }
            return this;
        }

        @Override
        public Item decode(ResultSet rs) throws SQLException
        {
            this.id = rs.getInt(1);
            this.value = rs.getString(2);
            return this;
        }
    }
}
