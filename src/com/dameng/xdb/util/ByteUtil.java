/*
 * @(#)ByteUtil.java, 2015年11月5日 上午9:46:25
 *
 * Copyright (c) 2000-2015, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.util;

import java.io.UnsupportedEncodingException;

/**
 * 字节操作工具类，小端模式
 * 
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public class ByteUtil
{
    public static int setByte(byte[] bytes, int offset, byte b)
    {
        bytes[offset] = b;
        return 1;
    }

    public static int setShort(byte[] bytes, int offset, short s)
    {
        bytes[offset++] = (byte)(s);
        bytes[offset++] = (byte)(s >> 8);
        return 2;
    }

    public static int setInt(byte[] bytes, int offset, int i)
    {
        bytes[offset++] = (byte)(i);
        bytes[offset++] = (byte)(i >> 8);
        bytes[offset++] = (byte)(i >> 16);
        bytes[offset++] = (byte)(i >> 24);
        return 4;
    }

    public static int setLong(byte[] bytes, int offset, long l)
    {
        bytes[offset++] = (byte)(l);
        bytes[offset++] = (byte)(l >> 8);
        bytes[offset++] = (byte)(l >> 16);
        bytes[offset++] = (byte)(l >> 24);
        bytes[offset++] = (byte)(l >> 32);
        bytes[offset++] = (byte)(l >> 40);
        bytes[offset++] = (byte)(l >> 48);
        bytes[offset++] = (byte)(l >> 56);
        return 8;
    }

    public static int setFloat(byte[] bytes, int offset, float f)
    {
        return setInt(bytes, offset, Float.floatToIntBits(f));
    }

    public static int setDouble(byte[] bytes, int offset, double d)
    {
        return setLong(bytes, offset, Double.doubleToLongBits(d));
    }

    public static int setUB1(byte[] bytes, int offset, int i)
    {
        bytes[offset] = (byte)i;
        return 1;
    }

    public static int setUB2(byte[] bytes, int offset, int i)
    {
        bytes[offset++] = (byte)(i);
        bytes[offset++] = (byte)(i >> 8);
        return 2;
    }

    public static int setUB3(byte[] bytes, int offset, int i)
    {
        bytes[offset++] = (byte)(i);
        bytes[offset++] = (byte)(i >> 8);
        bytes[offset++] = (byte)(i >> 16);
        return 3;
    }

    public static int setUB4(byte[] bytes, int offset, long l)
    {
        bytes[offset++] = (byte)(l);
        bytes[offset++] = (byte)(l >> 8);
        bytes[offset++] = (byte)(l >> 16);
        bytes[offset++] = (byte)(l >> 24);
        return 4;
    }

    public static int setBytes(byte[] bytes, int offset, byte[] srcBytes)
    {
        System.arraycopy(srcBytes, 0, bytes, offset, srcBytes.length);
        return srcBytes.length;
    }

    public static int setBytes(byte[] bytes, int offset, byte[] srcBytes, int srcOffset, int len)
    {
        System.arraycopy(srcBytes, srcOffset, bytes, offset, len);
        return len;
    }

    public static int setBytesWithLength(byte[] bytes, int offset, byte[] srcBytes)
    {
        offset += setInt(bytes, offset, srcBytes.length);
        return 4 + setBytes(bytes, offset, srcBytes);
    }

    public static int setBytesWithLength(byte[] bytes, int offset, byte[] srcBytes, int srcOffset, int len)
    {
        offset += setInt(bytes, offset, len);
        return 4 + setBytes(bytes, offset, srcBytes, srcOffset, len);
    }

    public static int setBytesWithLength2(byte[] bytes, int offset, byte[] srcBytes)
    {
        offset += setUB2(bytes, offset, srcBytes.length);
        return 2 + setBytes(bytes, offset, srcBytes);
    }

    public static int setBytesWithLength2(byte[] bytes, int offset, byte[] srcBytes, int srcOffset, int len)
    {
        offset += setUB2(bytes, offset, len);
        return 2 + setBytes(bytes, offset, srcBytes, srcOffset, len);
    }

    public static int setStringWithLength(byte[] bytes, int offset, String str, String encoding)
    {
        byte[] strBytes = getBytes(str, encoding);
        offset += setInt(bytes, offset, strBytes.length);
        return 4 + setBytes(bytes, offset, strBytes);
    }

    public static int setStringWithLength2(byte[] bytes, int offset, String str, String encoding)
    {
        byte[] strBytes = getBytes(str, encoding);
        offset += setUB2(bytes, offset, strBytes.length);
        return 2 + setBytes(bytes, offset, strBytes);
    }

    /************************************************/
    public static byte getByte(byte[] bytes, int offset)
    {
        return bytes[offset];
    }

    public static short getShort(byte[] bytes, int offset)
    {
        short val = (short)(bytes[offset++] & 0xff);
        val |= (short)(bytes[offset++] & 0xff) << 8;
        return val;
    }

    public static int getInt(byte[] bytes, int offset)
    {
        int val = bytes[offset++] & 0xff;
        val |= (bytes[offset++] & 0xff) << 8;
        val |= (bytes[offset++] & 0xff) << 16;
        val |= (bytes[offset++] & 0xff) << 24;
        return val;
    }

    public static long getLong(byte[] bytes, int offset)
    {
        long val = (long)(bytes[offset++] & 0xff);
        val |= (long)(bytes[offset++] & 0xff) << 8;
        val |= (long)(bytes[offset++] & 0xff) << 16;
        val |= (long)(bytes[offset++] & 0xff) << 24;
        val |= (long)(bytes[offset++] & 0xff) << 32;
        val |= (long)(bytes[offset++] & 0xff) << 40;
        val |= (long)(bytes[offset++] & 0xff) << 48;
        val |= (long)(bytes[offset++] & 0xff) << 56;
        return val;
    }

    public static float getFloat(byte[] bytes, int offset)
    {
        int ival = getInt(bytes, offset);
        return Float.intBitsToFloat(ival);
    }

    public static double getDouble(byte[] bytes, int offset)
    {
        long lval = getLong(bytes, offset);
        return Double.longBitsToDouble(lval);
    }

    public static int getUB1(byte[] bytes, int offset)
    {
        return bytes[offset] & 0xff;
    }

    public static int getUB2(byte[] bytes, int offset)
    {
        int i = bytes[offset++] & 0xff;
        i |= (bytes[offset++] & 0xff) << 8;
        return i;
    }

    public static int getUB3(byte[] bytes, int offset)
    {
        int i = bytes[offset++] & 0xff;
        i |= (bytes[offset++] & 0xff) << 8;
        i |= (bytes[offset++] & 0xff) << 16;
        return i;
    }

    public static long getUB4(byte[] bytes, int offset)
    {
        long l = (long)(bytes[offset++] & 0xff);
        l |= (long)(bytes[offset++] & 0xff) << 8;
        l |= (long)(bytes[offset++] & 0xff) << 16;
        l |= (long)(bytes[offset++] & 0xff) << 24;
        return l;
    }

    public static byte[] getBytesWithLength(byte[] bytes, int offset)
    {
        int len = getInt(bytes, offset);
        byte[] retBytes = new byte[len];
        System.arraycopy(bytes, offset + 4, retBytes, 0, len);
        return retBytes;
    }

    public static byte[] getBytesWithLength2(byte[] bytes, int offset)
    {
        int len = getUB2(bytes, offset);
        byte[] retBytes = new byte[len];
        System.arraycopy(bytes, offset + 2, retBytes, 0, len);
        return retBytes;
    }

    public static byte[] getBytes(byte[] bytes, int offset, int len)
    {
        byte[] retBytes = new byte[len];
        System.arraycopy(bytes, offset, retBytes, 0, len);
        return retBytes;
    }

    public static int getBytes(byte[] bytes, int offset, byte[] objBytes)
    {
        return getBytes(bytes, offset, objBytes, 0, objBytes.length);
    }

    public static int getBytes(byte[] bytes, int offset, byte[] objBytes, int objOffset, int len)
    {
        System.arraycopy(bytes, offset, objBytes, objOffset, len);
        return len;
    }

    public static byte[] getBytes(String str, String encoding)
    {
        if (str == null)
        {
            str = StringUtil.EMPTY;
        }
        byte[] strBytes = null;
        try
        {
            strBytes = str.getBytes(encoding);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("string encode fail", e);
        }
        return strBytes;
    }

    public static String getString(byte[] bytes, int offset, int len, String encoding)
    {
        String str = null;
        try
        {
            str = new String(bytes, offset, len, encoding);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("string decode fail", e);
        }
        return str;
    }

    public static String getStringWithLength(byte[] bytes, int offset, String encoding)
    {
        int len = getInt(bytes, offset);
        offset += 4;
        return getString(bytes, offset, len, encoding);
    }

    public static String getStringWithLength2(byte[] bytes, int offset, String encoding)
    {
        int len = getUB2(bytes, offset);
        offset += 2;
        return getString(bytes, offset, len, encoding);
    }

    public static int indexOf(byte[] subBytes, byte[] totalBytes)
    {
        int index = -1;
        int distance = totalBytes.length - subBytes.length;
        if (distance < 0)
        {
            return index;
        }

        int i, i1, j, j1;

        for (i = 0; i <= distance; i++)
        {
            for (j = 0; j < subBytes.length; j++)
            {
                i1 = subBytes[j] & 0xff;
                j1 = totalBytes[i + j] & 0xff;
                if (i1 != j1)
                {
                    break;
                }
            }

            if (j == subBytes.length)
            {
                index = i;
                break;
            }
        }

        return index;
    }

    public static void main(String[] args)
    {
        byte[] bytes = new byte[8];
        ByteUtil.setLong(bytes, 0, -128);
        System.out.println(ByteUtil.getLong(bytes, 0));

        ByteUtil.setUB4(bytes, 0, 4294967295L);
        System.out.println(ByteUtil.getUB4(bytes, 0));
    }
}
