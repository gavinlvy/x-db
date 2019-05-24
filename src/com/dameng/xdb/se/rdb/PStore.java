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
    public PStore(Connection connection)
    {
        super(connection);
    }

    @Override
    public void initialize() throws Exception
    {
        super.initialize();

        this.getStmt = this.connection
                .prepareStatement("select id, info, key, value, \"NEXT\" from rdb_p where id=?;");
        this.putStmt = this.connection
                .prepareStatement("insert into rdb_p(id, info, key, value, \"NEXT\") values(?, ?, ?, ?, ?);");
        this.setStmt = this.connection
                .prepareStatement("update rdb_p set info = ?, key = ?, value = ?, \"NEXT\" = ? where id = ?;");
        this.removeStmt = this.connection.prepareStatement("delete from rdb_p where id = ?;");
        this.showStmt = this.connection
                .prepareStatement("select top ? id, info, key, value, \"NEXT\" from rdb_p;");
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

    public static class P extends Item
    {
        public byte info;

        public int key;

        public long value;

        public int next;

        public P fill(byte info, int key, long value, int next)
        {
            this.info = info;
            this.key = key;
            this.value = value;
            this.next = next;
            return this;
        }

        public byte valueType()
        {
            return (byte)(this.info & IStorage.VALUE_TYPE_MASK);
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
                pstmt.setInt(3, this.key);
                pstmt.setLong(4, this.value);
                pstmt.setInt(5, this.next);
            }
            else if (type == Item.ENCODE_TYPE_UPDATE)
            {
                pstmt.setByte(1, this.info);
                pstmt.setInt(2, this.key);
                pstmt.setLong(3, this.value);
                pstmt.setInt(4, this.next);
                pstmt.setInt(5, this.id);
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
            this.key = rs.getInt(3);
            this.value = rs.getLong(4);
            this.next = rs.getInt(5);
            return this;
        }
    }
}
