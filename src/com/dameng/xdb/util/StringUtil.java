/*
 * @(#)StringUtil.java, 2018-4-12 上午08:51:54
 *
 * Copyright (c) 2000-2018, 达梦数据库有限公司.
 * All rights reserved.
 */
package com.dameng.xdb.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 在这里加入功能说明
 *
 * @author ychao
 * @version $Revision: $, $Author: $, $Date: $
 */
public final class StringUtil
{
    public static final String EMPTY = "";

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String LINUX_LINE_SEPARATOR = "\n";

    public static final String INVISIBLE_USERNAME = "……";

    public static final int yyyy_MM_dd_HH_mm_ss = 2;

    public static String trim(String str)
    {
        return str == null ? null : str.trim();
    }

    public static String rightTrim(String str)
    {
        if (str == null)
        {
            return null;
        }
        return ("r" + str).trim().substring(1);
    }

    public static String trimToEmpty(String str)
    {
        return str == null ? EMPTY : str.trim();
    }

    public static boolean isEmpty(String str)
    {
        return str == null || str.length() == 0;
    }

    public static boolean isNotEmpty(String str)
    {
        return str != null && str.length() > 0;
    }

    public static String substringBetween(String str, String tag)
    {
        return substringBetween(str, tag, tag);
    }

    public static String substringBetween(String str, String open, String close)
    {
        if (str == null || open == null || close == null)
        {
            return null;
        }
        int start = str.indexOf(open);
        if (start != -1)
        {
            int end = str.indexOf(close, start + open.length());
            if (end != -1)
            {
                return str.substring(start + open.length(), end);
            }
        }
        return null;
    }

    public static boolean equals(String str1, String str2)
    {
        return str1 == null ? str2 == null : str1.equals(str2);
    }

    public static boolean equalsIgnoreCase(String str1, String str2)
    {
        return str1 == null ? str2 == null : str1.equalsIgnoreCase(str2);
    }

    public static boolean isDigit(String str)
    {
        if (isEmpty(str))
        {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++)
        {
            if (Character.isDigit(str.charAt(i)))
            {
                continue;
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    /**
     * support Numeric format:<br>
     * "33" "+33" "033.30" "-.33" ".33" " 33." " 000.000 "
     * 
     * @param str String
     * @return boolean
     */
    public static boolean isNumerical(String str)
    {
        int begin = 0;
        boolean once = true;
        str = StringUtil.trimToEmpty(str);
        if (StringUtil.isEmpty(str))
        {
            return false;
        }
        if (str.startsWith("+") || str.startsWith("-"))
        {
            if (str.length() == 1)
            {
                // "+" "-"
                return false;
            }
            begin = 1;
        }
        for (int i = begin; i < str.length(); i++)
        {
            if (!Character.isDigit(str.charAt(i)))
            {
                if (str.charAt(i) == '.' && once)
                {
                    // '.' can only once
                    once = false;
                }
                else
                {
                    return false;
                }
            }
        }
        if (str.length() == (begin + 1) && !once)
        {
            // "." "+." "-."
            return false;
        }
        return true;
    }

    public static boolean isInteger(String str)
    {
        try
        {
            Integer.valueOf(str);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public static boolean isLong(String str)
    {
        try
        {
            Long.valueOf(str);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public static boolean isDouble(String str)
    {
        try
        {
            Double.valueOf(str);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public static String join(Object[] array)
    {
        return join(array, null);
    }

    public static String join(Object[] array, String separator)
    {
        if (array == null)
        {
            return null;
        }
        int arraySize = array.length;
        int bufSize = (arraySize == 0 ? 0
                : ((array[0] == null ? 16 : array[0].toString().length()) + 1) * arraySize);
        StringBuilder buf = new StringBuilder(bufSize);

        for (int i = 0; i < arraySize; i++)
        {
            if (i > 0)
            {
                buf.append(separator);
            }
            if (array[i] != null)
            {
                buf.append(array[i]);
            }
        }
        return buf.toString();
    }

    public static String replace(String text, String repl, String with, int maximum)
    {
        int max = maximum;

        if (text == null || isEmpty(repl) || with == null || max == 0)
        {
            return text;
        }

        StringBuilder buf = new StringBuilder(text.length());
        int start = 0, end = 0;
        while ((end = text.indexOf(repl, start)) != -1)
        {
            buf.append(text.substring(start, end)).append(with);
            start = end + repl.length();

            if (--max == 0)
            {
                break;
            }
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    public static String replaceAll(String text, String regex, String replacement, boolean caseSensitive)
    {
        Pattern pattern;
        if (!caseSensitive)
        {
            pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        }
        else
        {
            pattern = Pattern.compile(regex);
        }
        return pattern.matcher(text).replaceAll(replacement);
    }

    public static String replaceAllToUpperCase(String text, String regex, boolean caseSensitive)
    {
        Pattern pattern;
        if (!caseSensitive)
        {
            pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        }
        else
        {
            pattern = Pattern.compile(regex);
        }
        Matcher matcher = pattern.matcher(text);
        StringBuffer sb = new StringBuffer();
        int lastEndIndex = 0;
        while (matcher.find())
        {
            sb.append(text.substring(lastEndIndex, matcher.start()));
            sb.append(matcher.group().toUpperCase());
            lastEndIndex = matcher.end();
        }
        sb.append(text.substring(lastEndIndex));
        return sb.toString();
    }

    public static boolean isValidIP(String ip)
    {
        // String regEx =
        // "(((1[0-9]{1,2}|2([0-4][0-9]|5[0-5])|[1-9]{0,1}[0-9]{0,1})\\.){3}(1[0-9]{1,2}|2([0-4][0-9]|5[0-5])|[1-9]{0,1}[0-9]{0,1}))";
        if ("localhost".equalsIgnoreCase(ip))
        {
            return true;
        }
        else
        {
            String regEx = "((1[0-9]{1,2}|2([0-4][0-9]|5[0-5])|[0-9\\*]{1,2})\\.(1[0-9]{1,2}|2([0-4][0-9]|5[0-5])|[0-9\\*]{1,2})\\.(1[0-9]{1,2}|2([0-4][0-9]|5[0-5])|[0-9\\*]{1,2})\\.([1][0-9]{1,2}|2([0-4][0-9]|5[0-5])|[0-9\\*]{1,2}))";

            Pattern p = Pattern.compile(regEx);

            Matcher m = p.matcher(ip);

            return m.find() && m.group().length() == ip.length();
        }
    }

    public static boolean isValidMac(String mac)
    {
        Pattern macPattern = Pattern.compile("([0-9A-Fa-f]{2})(-[0-9A-Fa-f]{2}){5}");
        return macPattern.matcher(mac).matches();
    }

    public static boolean isValidEmail(String email)
    {
        // bug 524623
        Pattern pattern = Pattern.compile(
                "^[A-Z0-9a-z._%+-]+@(([A-Z0-9a-z._]+\\.[A-Za-z]{2,})|(([0-9]{1,3}\\.){3}[0-9]{1,3}))$");
        return pattern.matcher(email).matches();
    }

    public static final String bytesToHexString(byte[] bs)
    {
        return bytesToHexString(bs, false);
    }

    public static final String bytesToHexString(byte[] bs, boolean pre)
    {
        if (bs == null)
        {
            return null;
        }
        if (bs.length == 0)
        {
            return "";
        }
        String hexDigits = "0123456789ABCDEF";
        StringBuilder ret = new StringBuilder(bs.length * 2);
        for (byte b : bs)
        {
            ret.append(hexDigits.charAt(0x0F & (b >> 4)));
            ret.append(hexDigits.charAt(0x0F & b));
        }
        if (pre)
        {
            return "0x" + ret.toString();
        }
        return ret.toString();
    }

    public static final byte[] hexStringToBytes(String s)
    {
        String str = s;

        if (str == null)
        {
            return null;
        }

        byte[] bs = new byte[0];
        boolean flag = false;

        str = str.trim();
        if (str.indexOf("0x") == 0 || str.indexOf("0X") == 0)
        {
            str = str.substring(2, str.length());
        }

        if (str.length() == 0)
        {
            return bs;
        }

        char[] chr = str.toCharArray();
        char[] bsChr;
        int len = chr.length;
        int i;

        if (len % 2 == 0)
        {
            bsChr = chr;
        }
        else
        {
            len += 1;
            bsChr = new char[len];
            bsChr[0] = '0';
            for (i = 0; i < len - 1; i++)
            {
                bsChr[i + 1] = chr[i];
            }
        }
        bs = new byte[len / 2];

        int pos = 0;
        byte bt, bt2;
        for (i = 0; i < bsChr.length; i += 2)
        {
            bt = convertHex(bsChr[i]);
            bt2 = convertHex(bsChr[i + 1]);
            if (bt == -1 || bt2 == -1)
            {
                flag = true;
                break;
            }
            bs[pos++] = (byte)(bt * 16 + bt2);
        }

        if (flag)
        {
            bs = str.getBytes();
        }

        return bs;
    }

    private static byte convertHex(char chr)
    {
        byte us;
        if (chr >= '0' && chr <= '9')
        {
            us = (byte)(chr - '0');
        }
        else if (chr >= 'a' && chr <= 'f')
        {
            us = (byte)(chr - 'a' + 10);
        }
        else if (chr >= 'A' && chr <= 'F')
        {
            us = (byte)(chr - 'A' + 10);
        }
        else
        {
            return -1;
        }
        return us;
    }

    public static String md5(String s)
    {
        try
        {
            byte[] strTemp = s.getBytes();
            MessageDigest mdTemp = MessageDigest.getInstance("MD5");
            mdTemp.update(strTemp);
            byte[] md = mdTemp.digest();
            return bytesToHexString(md);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static boolean startWithIgnoreCase(String str1, String str2)
    {
        return str1 == null ? str2 == null : str1.toUpperCase().startsWith(str2.toUpperCase());
    }

    // 判断contains 忽略大小写
    public static boolean containsIgnoreCase(String str1, String str2)
    {
        return str1.toUpperCase().contains(str2.toUpperCase());
    }

    public static String formatCharset(String s, String srcCharset, String desCharset)
            throws UnsupportedEncodingException
    {
        String charset = null;
        if (srcCharset.equalsIgnoreCase("cp850"))
        {
            charset = "iso-8859-1";
            return new String(s.getBytes(charset), desCharset);
        }
        else
        {
            return s;
        }
    }

    public static String formatString(String format, Object... args)
    {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format(format, args);
        formatter.close();
        return sb.toString();
    }

    public static int ipToInt(String ip)
    {
        String[] splits = ip.split("\\.");
        return (Integer.valueOf(splits[0]) << 24) + (Integer.valueOf(splits[1]) << 16)
                + (Integer.valueOf(splits[2]) << 8) + Integer.valueOf(splits[3]);
    }

    public static String intToIp(int ip)
    {
        StringBuilder ipStr = new StringBuilder();
        ipStr.append((ip >> 24) & 0x000000FF);
        ipStr.append(".");
        ipStr.append((ip >> 16) & 0x000000FF);
        ipStr.append(".");
        ipStr.append((ip >> 8) & 0x000000FF);
        ipStr.append(".");
        ipStr.append(ip & 0x000000FF);

        return ipStr.toString();
    }

    public static String toPersent(double radio)
    {
        radio *= 100;
        String radioStr = Double.toString(radio);
        if (radioStr.length() > 6)
        {
            radioStr = radioStr.substring(0, 6);
        }
        return radioStr + "%";
    }

    public static String upperFirstChar(String str)
    {
        char firstChar = str.charAt(0);
        if (firstChar > 96 && firstChar < 123)
        {
            char[] strChars = str.toCharArray();
            strChars[0] = (char)((int)firstChar - 32);
            str = new String(strChars);
        }
        return str;
    }

    private static int compareVersion(String version1, String version2)
    {
        version1 = version1.replace("V", "").replace("v", "");
        version2 = version2.replace("V", "").replace("v", "");

        String[] version1Arr = version1.split("\\.");
        String[] version2Arr = version2.split("\\.");
        if (version1Arr.length == 0)
        {
            return -1;
        }

        for (int i = 0; i < version1Arr.length; i++)
        {
            if (i == version2Arr.length)
            {
                return 1;
            }
            int version1Int = Integer.valueOf(version1Arr[i]);
            int version2Int = Integer.valueOf(version2Arr[i]);
            if (version1Int > version2Int)
            {
                return 1;
            }
            else if (version1Int < version2Int)
            {
                return -1;
            }
        }

        if (version1Arr.length < version2Arr.length)
        {
            return -1;
        }
        return 0;
    }

    public static boolean isLastVersion(String version1, String version2)
    {
        return compareVersion(version1, version2) >= 0;
    }

    public static String getAlignNumString(int num, int maxNum)
    {
        StringBuilder sb = new StringBuilder();
        if (num >= maxNum || num < 0)
        {
            return String.valueOf(num);
        }
        while (maxNum >= 1)
        {
            maxNum = maxNum / 10;
            if (num >= 1)
            {
                sb.append(num % 10);
                num = num / 10;
            }
            else
            {
                sb.append("0");
            }
        }
        return sb.reverse().toString();
    }

    // 用来检查时区的正确性 现在时区的范围为(-12:59-+14:00)
    public static boolean isValidTimeZone(String timeZoneStr)
    {
        if (StringUtil.isEmpty(timeZoneStr))
        {
            return false;
        }
        if (timeZoneStr.length() != 6)
        {
            return false;
        }
        char flagChar = timeZoneStr.charAt(0);
        if (flagChar != '+' && flagChar != '-')
        {
            return false;
        }

        char fgChar = timeZoneStr.charAt(3);
        if (fgChar != ':')
        {
            return false;
        }

        String hourStr = timeZoneStr.substring(1, 3);
        String minuteStr = timeZoneStr.substring(4, 6);
        try
        {
            int hour = Integer.parseInt(hourStr);
            if (hour < 0 || hour > 14)
            {
                return false;
            }
            int minute = Integer.parseInt(minuteStr);
            if (minute < 0 || minute > 60)
            {
                return false;
            }

            int flagInt = Integer.parseInt("" + (flagChar == '+' ? "" : "-") + (hour * 60 + minute));
            return flagInt <= 840 && flagInt >= -779;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public static String removeEndSeparatorOfPath(String path, boolean isWin)
    {
        int pathLength = path.length();
        if (isWin && pathLength == 3)
        {
            return path;
        }
        else if (!isWin && pathLength == 1)
        {
            return path;
        }

        while ((isWin && path.charAt(pathLength - 1) == '\\') || (path.charAt(pathLength - 1) == '/'))
        {
            path = path.substring(0, path.length() - 1);
            pathLength = path.length();
        }
        return path;
    }

    public static String addEndSeparatorOfPath(String path, boolean isWin)
    {
        int pathLength = path.length();
        if ((isWin && path.charAt(pathLength - 1) == '\\') || (path.charAt(pathLength - 1) == '/'))
        {
            return path;
        }
        else
        {
            path = path + (isWin ? "\\" : "/");
        }
        return path;
    }

    public static String processDoubleQuoteOfName(String name)
    {
        return processQuoteOfName(name, "\"");
    }

    public static String processDoubleQuoteOfNameForLink(String name)
    {
        return "\"" + processDoubleQuoteOfName(name) + "\"";
    }

    public static String processSingleQuoteOfName(String name)
    {
        return processQuoteOfName(name, "'");
    }

    public static String processQuoteOfName(String name, String quote)
    {
        if (StringUtil.isEmpty(quote) || name == null)
        {
            return name;
        }
        String temp = name;
        StringBuilder result = new StringBuilder();
        int index = -1;
        int quetoLength = quote.length();
        while ((index = temp.indexOf(quote)) != -1)
        {
            result.append(temp.substring(0, index + quetoLength)).append(quote);
            temp = temp.substring(index + quetoLength);
        }
        result.append(temp);
        return result.toString();
    }

    public static String toString(Object obj)
    {
        if (obj == null)
        {
            return null;
        }
        else
        {
            return String.valueOf(obj);
        }
    }

    // 检查密码复杂度
    public static boolean checkPwdComplexity(String pwdStr)
    {
        boolean hasNum = false;
        boolean hasUpperChar = false;
        boolean hasLowerChar = false;
        boolean hasSpecialChar = false;

        if (null == pwdStr || pwdStr.length() == 0)
        {
            return false;
        }

        char[] pwdCharArray = pwdStr.toCharArray();
        for (char pwdChar : pwdCharArray)
        {
            if (pwdChar >= 32 && pwdChar <= 126)
            {
                if (pwdChar >= 48 && pwdChar <= 57)
                {
                    hasNum = true;
                }
                else if (pwdChar >= 65 && pwdChar <= 90)
                {
                    hasUpperChar = true;
                }
                else if (pwdChar >= 97 && pwdChar <= 122)
                {
                    hasLowerChar = true;
                }
                else
                {
                    hasSpecialChar = true;
                }
            }

            if (hasNum && hasUpperChar && hasLowerChar && hasSpecialChar)
            {
                return true;
            }
        }
        return false;
    }

    private static int min(int one, int two, int three)
    {
        int min = one;
        if (two < min)
        {
            min = two;
        }
        if (three < min)
        {
            min = three;
        }
        return min;
    }

    /*
     * 获取两个字符创建的编辑距离
     */
    public static int ld(String str1, String str2)
    {
        int d[][]; // 矩阵
        int n = str1.length();
        int m = str2.length();
        int i; // 遍历str1的
        int j; // 遍历str2的
        char ch1; // str1的
        char ch2; // str2的
        int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        if (n == 0)
        {
            return m;
        }
        if (m == 0)
        {
            return n;
        }
        d = new int[n + 1][m + 1];
        for (i = 0; i <= n; i++)
        { // 初始化第一列
            d[i][0] = i;
        }
        for (j = 0; j <= m; j++)
        { // 初始化第一行
            d[0][j] = j;
        }
        for (i = 1; i <= n; i++)
        { // 遍历str1
            ch1 = str1.charAt(i - 1);
            // 去匹配str2
            for (j = 1; j <= m; j++)
            {
                ch2 = str2.charAt(j - 1);
                if (ch1 == ch2)
                {
                    temp = 0;
                }
                else
                {
                    temp = 1;
                }
                // 左边+1,上边+1, 左上角+temp取最小
                d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp);
            }
        }
        return d[n][m];
    }

    /*
     * 获取两个字符创建的相似度（编辑距离的长短）
     */
    public static double sim(String str1, String str2)
    {
        int ld = ld(str1, str2);
        return 1 - (double)ld / Math.max(str1.length(), str2.length());
    }

    /**
     * 格式化成结果集表格输出，数据量比较大时建议分批调用； eg: String[] titles = {"ID", "SQL", "DESC"};
     * String[] fields = titles; Map<String, String> obj = new HashMap<String,
     * String>(); obj.put(fields[0], "123"); obj.put(fields[1], "select 1 from
     * dual;"); obj.put(fields[2], "Dameng Database"); List<Map> objList = new
     * ArrayList<Map>(); objList.add(obj); System.out.println(toTable(objList,
     * titles, fields, 50, false));
     */
    @SuppressWarnings ("rawtypes")
    public static String format2Table(List<Map> objList, String[] titles, String[] fields, int maxColLen,
            boolean showAll)
    {
        if (objList == null || fields == null)
        {
            return "";
        }

        // calculate all the column length
        int colLen = 0;
        String colVal = null;
        int[] colLens = new int[fields.length];
        for (int i = 0; i < fields.length; ++i)
        {
            colLens[i] = fields[i].length();
        }
        for (Map obj : objList)
        {
            for (int i = 0; i < fields.length; ++i)
            {
                Object tobj = obj.get(fields[i]);
                if (tobj == null)
                {
                    colVal = "null";
                }
                else
                {
                    colVal = tobj.toString();
                }
                colLen = colVal.length();
                if (colLen > colLens[i])
                {
                    colLens[i] = colLen;
                }
            }
        }
        for (int i = 0; i < fields.length; ++i)
        {
            if (maxColLen > 0 && colLens[i] > maxColLen)
            {
                colLens[i] = maxColLen;
            }
        }

        // formate output
        StringBuilder output = new StringBuilder();

        if (titles != null)
        {
            Map<String, String> titleMap = new HashMap<String, String>(titles.length);
            for (int i = 0; i < titles.length; ++i)
            {
                titleMap.put(fields[i], titles[i]);
            }
            objList.add(0, titleMap);
        }

        sepLine(output, colLens);

        for (Map obj : objList)
        {
            formateLine(output, obj, fields, colLens, showAll);
            sepLine(output, colLens);
        }

        return output.toString();
    }

    @SuppressWarnings ({"rawtypes", "unchecked"})
    private static void formateLine(StringBuilder output, Map obj, String[] fields, int[] colLens,
            boolean showAll)
    {
        boolean hasMore = false;

        int colLen = 0;
        String colVal = null;
        for (int i = 0; i < fields.length; ++i)
        {
            Object tobj = obj.get(fields[i]);
            if (tobj == null)
            {
                colVal = "null";
            }
            else
            {
                colVal = tobj.toString();
                colVal = colVal.replace('\t', ' ');
                colVal = colVal.replace('\n', ' ');
                colVal = colVal.replace('\r', ' ');
            }

            colLen = colVal.length();
            if (colLen <= colLens[i])
            {
                output.append('|');
                output.append(colVal);
                blanks(output, colLens[i] - colLen);
                if (showAll)
                {
                    obj.put(fields[i], "");
                }
            }
            else
            {
                output.append('|');
                if (showAll)
                {
                    output.append(colVal.substring(0, colLens[i]));
                    obj.put(fields[i], colVal.substring(colLens[i]));
                    hasMore = true;
                }
                else
                {
                    output.append(colVal.substring(0, colLens[i] - 3)).append("...");
                }
            }
        }
        output.append('|');
        output.append('\n');

        if (hasMore)
        {
            formateLine(output, obj, fields, colLens, showAll);
        }
    }

    private static void sepLine(StringBuilder output, int[] colLens)
    {
        output.append('+');
        for (int colLen : colLens)
        {
            for (int i = 0; i < colLen; ++i)
            {
                output.append('-');
            }
            output.append('+');
        }
        output.append('\n');
    }

    private static void blanks(StringBuilder output, int count)
    {
        while (count > 0)
        {
            output.append(' ');
            count--;
        }
    }

    /**
     * 获取服务器的svn版本号; 解析方法，获取括号内第一个-后面的一组数字字符; DM Database Server x64
     * V7.1.5.71-Build(2016.04.12-67512)ENT; DM Database Server x64
     * V7.1.5.71-Build(2016.04.12-67512-debug); DM Database Server x64
     * V7.1.5.71-Build(2016.04.12-67512trunc); DM Database Server x64
     * V7.1.5.71-Build(2016.04.12-67512trunc-debug)
     */
    public static long parseDBSvnNumber(String svrStr)
    {
        long currentSvr = 0L;
        if (StringUtil.isEmpty(svrStr))
        {
            currentSvr = 0L;
        }
        else
        {

            if (svrStr.lastIndexOf("(") < 0 || svrStr.lastIndexOf(")") < 0)
            {
                return currentSvr;
            }
            svrStr = svrStr.substring(svrStr.lastIndexOf("(") + 1, svrStr.lastIndexOf(")"));
            if (!svrStr.contains("-"))
            {
                return currentSvr;
            }
            svrStr = svrStr.substring(svrStr.indexOf("-") + 1, svrStr.length());

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < svrStr.length(); i++)
            {
                if ('0' <= svrStr.charAt(i) && '9' >= svrStr.charAt(i))
                {
                    sb.append(svrStr.charAt(i));
                }
                else
                {
                    break;
                }
            }
            currentSvr = Long.valueOf(sb.toString());
        }
        return currentSvr;
    }

    public static void appendLine(StringBuilder sb)
    {
        sb.append(LINE_SEPARATOR);
    }

    public static void appand(StringBuilder sb, int tabNum, String str)
    {
        for (int i = 0; i < tabNum; i++)
        {
            sb.append("\t");
        }
        sb.append(str);
        appendLine(sb);
    }

    // 解析方法，获取括号内第一个-后面的一组数字字符
    // DM Database Server x64 V7.1.5.71-Build(2016.04.12-67512)ENT
    // DM Database Server x64 V7.1.5.71-Build(2016.04.12-67512-debug)
    // DM Database Server x64 V7.1.5.71-Build(2016.04.12-67512trunc)
    // DM Database Server x64 V7.1.5.71-Build(2016.04.12-67512trunc-debug)
    public static long getCurrentSvrNumber(String svrStr)
    {
        long currentSvr = 0L;
        if (isNotEmpty(svrStr))
        {
            if (svrStr.lastIndexOf("(") < 0 || svrStr.lastIndexOf(")") < 0)
            {
                return currentSvr;
            }
            svrStr = svrStr.substring(svrStr.lastIndexOf("(") + 1, svrStr.lastIndexOf(")"));
            if (!svrStr.contains("-"))
            {
                return currentSvr;
            }
            svrStr = svrStr.substring(svrStr.indexOf("-") + 1, svrStr.length());

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < svrStr.length(); i++)
            {
                if ('0' <= svrStr.charAt(i) && '9' >= svrStr.charAt(i))
                {
                    sb.append(svrStr.charAt(i));
                }
                else
                {
                    break;
                }
            }
            currentSvr = Long.valueOf(sb.toString());
        }
        return currentSvr;
    }

    public static String replaceSpecialCharFromShell(String str)
    {
        if (isEmpty(str))
        {
            return "";
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < str.length(); i++)
        {
            char tmpChar = str.charAt(i);
            if (tmpChar == '$' || tmpChar == '"' || tmpChar == '\\')
            {
                result.append('\\');
            }
            result.append(tmpChar);
        }
        return result.toString();
    }

    public static long getSizeWithoutUnit(String spaceStr)
    {
        if (isLong(spaceStr))
        {
            return Long.valueOf(spaceStr);
        }

        if (spaceStr.length() <= 1)
        {
            return 0;
        }
        String unitStr = spaceStr.substring(spaceStr.length() - 1);
        String spaceSizeStr = spaceStr.substring(0, spaceStr.length() - 1);
        if (unitStr.equalsIgnoreCase("K"))
        {
            return Long.parseLong(spaceSizeStr) * 1024;
        }
        else if (unitStr.equalsIgnoreCase("M"))
        {
            return Long.parseLong(spaceSizeStr) * 1024 * 1024;
        }
        else if (unitStr.equalsIgnoreCase("G"))
        {
            return Long.parseLong(spaceSizeStr) * 1024 * 1024 * 1024;
        }
        else if (unitStr.equalsIgnoreCase("T"))
        {
            return Long.parseLong(spaceSizeStr) * 1024 * 1024 * 1024 * 1024;
        }
        return 0;
    }

    public static String getSizeWithUnit(long size)
    {
        return getSizeWithUnit(size, "");
    }

    public static String getSizeWithUnit(long size, String unitStr)
    {
        String strSize = "";
        if (size / 1024 > 2)
        {
            size /= 1024;
            strSize = String.valueOf(size) + "K";
            if (unitStr.equalsIgnoreCase("K"))
            {
                return strSize;
            }
        }
        if (size / 1024 > 2)
        {
            size /= 1024;
            strSize = String.valueOf(size) + "M";
            if (unitStr.equalsIgnoreCase("M"))
            {
                return strSize;
            }
        }
        if (size / 1024 > 2)
        {
            size /= 1024;
            strSize = String.valueOf(size) + "G";
            if (unitStr.equalsIgnoreCase("G"))
            {
                return strSize;
            }
        }
        if (size / 1024 > 2)
        {
            size /= 1024;
            strSize = String.valueOf(size) + "T";
            if (unitStr.equalsIgnoreCase("T"))
            {
                return strSize;
            }
        }
        return strSize;
    }

    public static String formatDir(String dir)
    {
        dir = trimToEmpty(dir);
        if (isNotEmpty(dir))
        {
            if (!dir.endsWith(File.separator))
            {
                dir += File.separator;
            }
        }
        return dir;
    }

    public static final String[] split(String s, String seperators)
    {
        if (s == null || seperators == null)
        {
            return null;
        }

        int[] foot = new int[s.length()]; // 足够的元素个数
        int count = 0; // 实际元素个数
        int sLen = s.length();
        int seperatorsLen = seperators.length();
        for (int i = 0; i < sLen; i++)
        {
            // 处理 s == “-9999-1" && seperators == "-"情况
            if (i == 0 && sLen >= seperatorsLen)
            {
                if (s.substring(0, seperatorsLen).equals(seperators))
                {
                    i += seperatorsLen - 1;
                    continue;
                }
            }

            for (int j = 0; j < seperatorsLen; j++)
            {
                if (s.charAt(i) == seperators.charAt(j))
                {
                    foot[count] = i;
                    count++;
                    break;
                }
            }
        }

        String[] ret = new String[count + 1];
        if (count == 0)
        {
            ret[0] = s;
            return ret;
        }

        ret[0] = s.substring(0, foot[0]);
        for (int i = 1; i < count; i++)
        {
            ret[i] = s.substring(foot[i - 1] + 1, foot[i]);
        }
        ret[count] = s.substring(foot[count - 1] + 1, s.length());
        return ret;
    }

    @SuppressWarnings ("deprecation")
    public static String formatDateTimeString(Date date, int style)
    {
        String value = null;
        int year = date.getYear();
        int month = date.getMonth();
        String day = getStrFromInt(date.getDate());
        String hour = getStrFromInt(date.getHours());
        String minute = getStrFromInt(date.getMinutes());
        String second = getStrFromInt(date.getSeconds());
        switch (style)
        {
            case yyyy_MM_dd_HH_mm_ss:
                value = "" + (year + 1900) + "-" + getStrFromInt((month + 1)) + "-" + day + " " + hour + ":"
                        + minute + ":" + second;
                break;

            default:
                value = date.toString();
                break;
        }
        return value;
    }

    private static String getStrFromInt(int i)
    {
        return i < 10 ? "0" + i : String.valueOf(i);
    }

    public static void main(String[] args)
    {
        // System.out.print(new String(hexStringToBytes("0D0A")));
        // System.out.print("val: ");
    }
}
