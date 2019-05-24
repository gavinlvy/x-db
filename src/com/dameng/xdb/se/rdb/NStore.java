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

import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class NStore extends Store
{
    public NStore(Connection connection)
    {
        super(connection);
    }

    @Override
    public void initialize() throws Exception
    {
        super.initialize();

        this.getStmt = this.connection
                .prepareStatement("select id, info, prop, \"LINK\" from rdb_n where id = ?;");
        this.putStmt = this.connection
                .prepareStatement("insert into rdb_n(id, info, prop, \"LINK\") values(?, ?, ?, ?);");
        this.setStmt = this.connection
                .prepareStatement("update rdb_n set info = ?, prop = ?, \"LINK\" = ? where id = ?;");
        this.removeStmt = this.connection.prepareStatement("delete from rdb_n where id = ?;");
        this.showStmt = this.connection.prepareStatement("select top ? id, info, prop, \"LINK\" from rdb_n;");
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

    public static class N extends Item
    {
        public byte info;

        public int prop;

        public int link;

        public N fill(byte info, int prop, int link)
        {
            this.info = info;
            this.prop = prop;
            this.link = link;
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
                pstmt.setByte(2, this.info);
                pstmt.setInt(3, this.prop);
                pstmt.setInt(4, this.link);
            }
            else if (type == Item.ENCODE_TYPE_UPDATE)
            {
                pstmt.setByte(1, this.info);
                pstmt.setInt(2, this.prop);
                pstmt.setInt(3, this.link);
                pstmt.setInt(4, this.id);
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
            this.info = rs.getByte(2);
            this.prop = rs.getInt(3);
            this.link = rs.getInt(4);
            return this;
        }
    }
}
