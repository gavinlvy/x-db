/*
 * @(#)MSG.java, 2018年9月10日 上午10:58:14
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.se.driver.msg;

import java.util.function.BiConsumer;

import com.dameng.xdb.XDBException;
import com.dameng.xdb.se.model.GObject;
import com.dameng.xdb.se.model.Link;
import com.dameng.xdb.se.model.Node;
import com.dameng.xdb.se.model.PropValue;
import com.dameng.xdb.util.buffer.Buffer;

/**
 * head format(32):
 * CRC(1) + compress(1) + encrypt(1) + command(1) + length(4) + sql_code(4) + reserved(20)
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public abstract class MSG
{
    // message format
    public final static int NET_PACKET_SIZE = 8092;

    public final static int HEAD_LENGTH = 32;

    public final static int OFFSET_CRC = 0;

    public final static int OFFSET_COMPRESS = OFFSET_CRC + 1;

    public final static int OFFSET_ENCRYPT = OFFSET_COMPRESS + 1;

    public final static int OFFSET_COMMAND = OFFSET_ENCRYPT + 1;

    public final static int OFFSET_LENGTH = OFFSET_COMMAND + 1;

    public final static int OFFSET_SQL_CODE = OFFSET_LENGTH + 4;

    // command
    public final static byte COMMAND_DUMMY = 0;

    public final static byte COMMAND_CONNECT = 1;

    public final static byte COMMAND_PUT = 2;

    public final static byte COMMAND_GET = 3;

    public final static byte COMMAND_SET = 4;

    public final static byte COMMAND_REMOVE = 5;

    // some declare
    public final static byte GOBJ_TYPE_MASK = (byte)0x80;

    public final static byte GOBJ_TYPE_NODE = (byte)0x00;

    public final static byte GOBJ_TYPE_LINK = (byte)0x80;

    public final static byte NULL_TRUE = (byte)0x80;

    public final static byte NULL_FALSE = (byte)0x00;

    public byte command;

    public Buffer buffer;

    public String encoding;

    public XDBException exception;

    public MSG(byte command, Buffer buffer, String encoding)
    {
        this.command = command;
        this.buffer = buffer;
        this.encoding = encoding;
    }

    public void encode()
    {
        beforeEncode();

        try
        {
            doEncode();
        }
        finally
        {
            afterEncode();
        }
    }

    protected void beforeEncode()
    {
        buffer.clear();
        buffer.fill(HEAD_LENGTH);
    }

    protected abstract void doEncode();

    protected void afterEncode()
    {
        buffer.setShort(OFFSET_COMMAND, command);
        buffer.setInt(OFFSET_LENGTH, buffer.length() - HEAD_LENGTH);
    }

    public void decode()
    {
        beforeDecode();

        try
        {
            doDecode();
        }
        finally
        {
            afterDecode();
        }
    }

    protected void beforeDecode()
    {
        buffer.flip();
        buffer.skip(HEAD_LENGTH);
    }

    protected abstract void doDecode();

    protected void afterDecode()
    {}

    public boolean encodeException()
    {
        if (exception == null)
        {
            buffer.setInt(OFFSET_SQL_CODE, 0);
        }
        else
        {
            buffer.setInt(OFFSET_SQL_CODE, exception.errCode);
            buffer.writeStringWithLength(exception.desc, encoding);
        }

        return exception != null;
    }

    public void decodeException()
    {
        int errCode = buffer.getInt(OFFSET_SQL_CODE);
        if (errCode > 0)
        {
            XDBException.throwException(errCode, buffer.getStringWithLength(HEAD_LENGTH, encoding));
        }
    }

    public void encodeIds(int... ids)
    {
        buffer.writeInt(ids.length);
        for (int i = 0; i < ids.length; ++i)
        {
            buffer.writeInt(ids[i]);
        }
    }

    public int[] decodeIds()
    {
        int[] ids = new int[buffer.readInt()];
        for (int i = 0; i < ids.length; ++i)
        {
            ids[i] = buffer.readInt();
        }
        return ids;
    }

    public void encodeRets(boolean... rets)
    {
        buffer.writeInt(rets.length);
        for (int i = 0; i < rets.length; ++i)
        {
            buffer.writeBoolean(rets[i]);
        }
    }

    public boolean[] decodeRets()
    {
        boolean[] rets = new boolean[buffer.readInt()];
        for (int i = 0; i < rets.length; ++i)
        {
            rets[i] = buffer.readBoolean();
        }
        return rets;
    }

    /**
     * info(1) + obj_count(4) + { null(1) | id(4) + category_count(1) + {category}* + key_value_count(4) + {key + value}* + {extra_info}? }+
     *      
     *      info(1)
     *        |- #node or link
     * 
     *      value: val_desc(1) + val_data(x)
     *                |- [xxxx,----] #type
     *                |- [----,xxxx] #length
     */
    public void encodeObjects(boolean node, boolean extra, GObject<?>... objs)
    {
        // objects
        buffer.writeInt(objs.length);
        for (GObject<?> obj : objs)
        {
            // null
            buffer.writeByte(obj == null ? NULL_TRUE : NULL_FALSE);
            if (obj == null)
            {
                continue;
            }

            // id
            buffer.writeInt(obj.id);

            // category
            buffer.writeUB1((byte)obj.categorys.length);
            for (String category : obj.categorys)
            {
                buffer.writeStringWithLength1(category, encoding);
            }

            // property
            buffer.writeInt(obj.propMap.size());
            obj.propMap.forEach(new BiConsumer<String, PropValue>()
            {
                @Override
                public void accept(String pkey, PropValue pval)
                {
                    // key
                    buffer.writeStringWithLength1(pkey, encoding);

                    // value
                    byte desc = (byte)(pval.type << 4);
                    switch (pval.type)
                    {
                        case PropValue.TYPE_NUMBERIC:
                        {
                            long value = (long)pval.value;
                            if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE)
                            {
                                desc |= (byte)1;
                                buffer.writeByte(desc);
                                buffer.writeByte((byte)value);
                            }
                            else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE)
                            {
                                desc |= (byte)2;
                                buffer.writeByte(desc);
                                buffer.writeShort((short)value);
                            }
                            else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE)
                            {
                                desc |= (byte)4;
                                buffer.writeByte(desc);
                                buffer.writeInt((int)value);
                            }
                            else
                            {
                                desc |= (byte)8;
                                buffer.writeByte(desc);
                                buffer.writeLong((long)value);
                            }
                            break;
                        }
                        case PropValue.TYPE_DECIMAL:
                        {
                            double value = (double)pval.value;
                            if (Float.MIN_VALUE <= value && value <= Float.MAX_VALUE)
                            {
                                desc |= (byte)4;
                                buffer.writeByte(desc);
                                buffer.writeFloat((float)value);
                            }
                            else
                            {
                                desc |= (byte)8;
                                buffer.writeByte(desc);
                                buffer.writeDouble((double)value);
                            }
                            break;
                        }
                        case PropValue.TYPE_BOOLEAN:
                        {
                            boolean value = (boolean)pval.value;
                            desc |= (byte)1;
                            buffer.writeByte(desc);
                            buffer.writeByte(value ? (byte)1 : (byte)0);
                            break;
                        }
                        default: // STRING
                        {
                            String value = (String)pval.value;
                            buffer.writeByte(desc);
                            buffer.writeStringWithLength(value, encoding);
                            break;
                        }
                    }
                }
            });

            // extra
            if (extra)
            {
                if (node)
                {
                    buffer.writeInt(((Node)obj).link);
                }
                else
                {
                    buffer.writeInt(((Link)obj).fnode);
                    buffer.writeInt(((Link)obj).tnode);
                }
            }
        }
    }

    public GObject<?>[] decodeObjects(boolean node, boolean extra)
    {
        // objects
        GObject<?>[] objs = node ? new Node[buffer.readInt()] : new Link[buffer.readInt()];
        for (int i = 0; i < objs.length; ++i)
        {
            // null
            boolean isNull = buffer.readByte() == NULL_TRUE;
            if (isNull)
            {
                objs[i] = null;
                continue;
            }

            objs[i] = node ? new Node() : new Link();

            // id
            objs[i].id = buffer.readInt();

            // category
            objs[i].categorys = new String[buffer.readUB1()];
            for (int j = 0; j < objs[i].categorys.length; ++j)
            {
                objs[i].categorys[j] = buffer.readStringWithLength1(encoding);
            }

            // property
            int propCount = buffer.readInt();
            for (int j = 0; j < propCount; ++j)
            {
                String key = buffer.readStringWithLength1(encoding);
                byte desc = buffer.readByte();
                byte type = (byte)(desc >>> 4);
                byte length = (byte)(desc & 0x0F);
                switch (type)
                {
                    case PropValue.TYPE_NUMBERIC:
                    {
                        if (length == 1)
                        {
                            objs[i].set(key, buffer.readByte());
                        }
                        else if (length == 2)
                        {
                            objs[i].set(key, buffer.readShort());
                        }
                        else if (length == 4)
                        {
                            objs[i].set(key, buffer.readInt());
                        }
                        else
                        {
                            objs[i].set(key, buffer.readLong());
                        }
                        break;
                    }
                    case PropValue.TYPE_DECIMAL:
                    {
                        if (length == 4)
                        {
                            objs[i].set(key, buffer.readFloat());
                        }
                        else
                        {
                            objs[i].set(key, buffer.readDouble());
                        }
                        break;
                    }
                    case PropValue.TYPE_BOOLEAN:
                    {
                        objs[i].set(key, buffer.readByte() == 0 ? false : true);
                        break;
                    }
                    default:
                    {
                        objs[i].set(key, buffer.readStringWithLength(encoding));
                        break;
                    }
                }
            }

            // extra
            if (extra)
            {
                if (node)
                {
                    ((Node)objs[i]).link = buffer.readInt();
                }
                else
                {
                    ((Link)objs[i]).fnode = buffer.readInt();
                    ((Link)objs[i]).tnode = buffer.readInt();
                }
            }
        }
        return objs;
    }
}
