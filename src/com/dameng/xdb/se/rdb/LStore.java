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

import com.dameng.xdb.util.MiscUtil;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class LStore extends Store
{
    public LStore(Connection connection)
    {
        super(connection);
    }

    @Override
    public void initialize() throws Exception
    {
        super.initialize();

        this.getStmt = this.connection
                .prepareStatement("select id, info, prop, fnode, tnode, fnode_prev, fnode_next, tnode_prev, tnode_next from rdb_l where id = ?");
        this.putStmt = this.connection
                .prepareStatement("insert into rdb_l(id, info, prop, fnode, tnode, fnode_prev, fnode_next, tnode_prev, tnode_next) values(?, ?, ?, ?, ?, ?, ?, ?, ?);");
        this.setStmt = this.connection
                .prepareStatement("update rdb_l set info = ?, prop = ?, fnode = ?, tnode = ?, fnode_prev = ?, fnode_next = ?, tnode_prev = ?, tnode_next = ? where id = ?;");
        this.removeStmt = this.connection.prepareStatement("delete from rdb_l where id = ?;");
        this.showStmt = this.connection
                .prepareStatement("select top ? id, info, prop, fnode, tnode, fnode_prev, fnode_next, tnode_prev, tnode_next from rdb_l;");
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

    public static class L extends Item
    {
        public byte info;

        public int prop;

        public int fnode;

        public int tnode;

        public int fnode_prev;

        public int fnode_next;

        public int tnode_prev;

        public int tnode_next;

        public L fill(byte info, int prop, int fnode, int tnode, int fnode_prev, int fnode_next,
                int tnode_prev, int tnode_next)
        {
            this.info = info;
            this.prop = prop;
            this.fnode = fnode;
            this.tnode = tnode;
            this.fnode_prev = fnode_prev;
            this.fnode_next = fnode_next;
            this.tnode_prev = tnode_prev;
            this.tnode_next = tnode_next;
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
                pstmt.setInt(4, this.fnode);
                pstmt.setInt(5, this.tnode);
                pstmt.setInt(6, this.fnode_prev);
                pstmt.setInt(7, this.fnode_next);
                pstmt.setInt(8, this.tnode_prev);
                pstmt.setInt(9, this.tnode_next);
            }
            else if (type == Item.ENCODE_TYPE_UPDATE)
            {
                pstmt.setByte(1, this.info);
                pstmt.setInt(2, this.prop);
                pstmt.setInt(3, this.fnode);
                pstmt.setInt(4, this.tnode);
                pstmt.setInt(5, this.fnode_prev);
                pstmt.setInt(6, this.fnode_next);
                pstmt.setInt(7, this.tnode_prev);
                pstmt.setInt(8, this.tnode_next);
                pstmt.setInt(9, this.id);
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
            this.fnode = rs.getInt(4);
            this.tnode = rs.getInt(5);
            this.fnode_prev = rs.getInt(6);
            this.fnode_next = rs.getInt(7);
            this.tnode_prev = rs.getInt(8);
            this.tnode_next = rs.getInt(9);
            return this;
        }
    }
}
