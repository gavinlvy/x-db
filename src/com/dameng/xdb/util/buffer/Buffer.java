/*
 * @(#)Buffer.java, 2018年9月5日 上午8:54:06
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.util.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class Buffer
{
    public static void main(String[] args)
    {
        Buffer buffer = Buffer.allocateBytes(8);
        buffer.fill(8);
        buffer.setByte(0, (byte)(Byte.MAX_VALUE + 3));
        System.out.println(buffer.getByte(0));
    }

    public final static Buffer wrap(byte[] bytes)
    {
        return new ByteArrayBuffer(bytes, 0, bytes.length);
    }

    public final static Buffer allocateBytes(int capacity)
    {
        return new ByteArrayBuffer(capacity);
    }

    public final static Buffer allocateHeap(int capacity)
    {
        return new ByteBufferBuffer(capacity, false);
    }

    public final static Buffer allocateDirect(int capacity)
    {
        return new ByteBufferBuffer(capacity, true);
    }

    /**
     * 清空buffer，调整buffer size & offset，内容保留
     */
    abstract public void clear();

    /**
     * 从指定偏移清理buffer
     */
    abstract public void truncate(int offset);

    /**
     * 计算buffer有效写入数据长度
     */
    abstract public int length();

    /**
     * 各Node节点光标移到起始位置（写转读）
     */
    abstract public void flip();

    /**
     * 指定长度以0填充（用于写入）
     */
    abstract public void fill(int len);

    /**
     * 跳过指定长度（用于读取）
     */
    abstract public void skip(int len);

    /**
     * 整体逻辑offset
     */
    abstract public long offset();

    /*---------------------------------------------------*/
    abstract public int writeBoolean(boolean b);
    
    abstract public int writeByte(byte b);

    abstract public int writeShort(short s);

    abstract public int writeInt(int i);

    abstract public int writeUB1(int i);

    abstract public int writeUB2(int i);

    abstract public int writeUB3(int i);

    abstract public int writeUB4(long l);

    abstract public int writeLong(long l);

    abstract public int writeFloat(float f);

    abstract public int writeDouble(double d);

    abstract public int writeBytes(byte[] srcBytes);

    abstract public int writeBytes(byte[] srcBytes, int srcOffset, int len);

    abstract public int writeBytesWithLength(byte[] srcBytes, int srcOffset, int len);

    abstract public int writeBytesWithLength1(byte[] srcBytes, int srcOffset, int len);

    abstract public int writeBytesWithLength2(byte[] srcBytes, int srcOffset, int len);

    abstract public int writeStringWithLength(String str, String encoding);

    abstract public int writeStringWithLength1(String str, String encoding);
    
    abstract public int writeStringWithLength2(String str, String encoding);

    abstract public int writeStringWithNTS(String str, String encoding);

    /*---------------------------------------------------*/
    abstract public boolean readBoolean();
    
    abstract public byte readByte();

    abstract public short readShort();

    abstract public int readInt();

    abstract public long readLong();

    abstract public float readFloat();

    abstract public double readDouble();

    abstract public int readUB1();

    abstract public int readUB2();

    abstract public int readUB3();

    abstract public long readUB4();

    abstract public byte[] readBytes(int len);

    abstract public byte[] readBytesWithLength();

    abstract public byte[] readBytesWithLength1();
    
    abstract public byte[] readBytesWithLength2();

    abstract public String readString(int len, String encoding);

    abstract public String readStringWithLength(String encoding);
    
    abstract public String readStringWithLength1(String encoding);

    abstract public String readStringWithLength2(String encoding);

    /*---------------------------------------------------*/
    abstract public int setBoolean(int offset, boolean b);
    
    abstract public int setByte(int offset, byte b);

    abstract public int setShort(int offset, short s);

    abstract public int setInt(int offset, int i);

    abstract public int setLong(int offset, long l);

    abstract public int setFloat(int offset, float f);

    abstract public int setDouble(int offset, double d);

    abstract public int setUB1(int offset, int i);

    abstract public int setUB2(int offset, int i);

    abstract public int setUB3(int offset, int i);

    abstract public int setUB4(int offset, long l);

    abstract public int setBytes(int offset, byte[] srcBytes);

    abstract public int setBytes(int offset, byte[] srcBytes, int srcOffset, int len);

    abstract public int setBytesWithLength(int offset, byte[] srcBytes);

    abstract public int setBytesWithLength(int offset, byte[] srcBytes, int srcOffset, int len);

    abstract public int setBytesWithLength1(int offset, byte[] srcBytes);
    
    abstract public int setBytesWithLength1(int offset, byte[] srcBytes, int srcOffset, int len);

    abstract public int setBytesWithLength2(int offset, byte[] srcBytes);

    abstract public int setBytesWithLength2(int offset, byte[] srcBytes, int srcOffset, int len);

    abstract public int setStringWithLength(int offset, String str, String encoding);

    abstract public int setStringWithLength1(int offset, String str, String encoding);
    
    abstract public int setStringWithLength2(int offset, String str, String encoding);

    abstract public int setStringWithNTS(int offset, String str, String encoding);

    /*---------------------------------------------------*/
    abstract public boolean getBoolean(int offset);
    
    abstract public byte getByte(int offset);

    abstract public short getShort(int offset);

    abstract public int getInt(int offset);

    abstract public long getLong(int offset);

    abstract public float getFloat(int offset);

    abstract public double getDouble(int offset);

    abstract public int getUB1(int offset);

    abstract public int getUB2(int offset);

    abstract public int getUB3(int offset);

    abstract public long getUB4(int offset);

    abstract public byte[] getBytes(int offset, int len);

    abstract public byte[] getBytesWithLength(int offset);
    
    abstract public byte[] getBytesWithLength1(int offset);

    abstract public byte[] getBytesWithLength2(int offset);

    abstract public String getStringWithLength(int offset, String encoding);

    abstract public String getStringWithLength1(int offset, String encoding);
    
    abstract public String getStringWithLength2(int offset, String encoding);

    /*---------------------------------------------------*/
    abstract public int load(InputStream is, int len) throws IOException;

    abstract public void flush(OutputStream os) throws IOException;

    abstract public int load(ReadableByteChannel channel, int len) throws IOException;

    abstract public void flush(WritableByteChannel channel) throws IOException;
}
