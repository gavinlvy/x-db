package com.dameng.xdb.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import com.dameng.xdb.XDBException;

public class DescriptiveErrorListener extends BaseErrorListener
{
    public static DescriptiveErrorListener INSTANCE = new DescriptiveErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
            int charPositionInLine, String msg, RecognitionException e)
    {
        XDBException.SE_SYNTEX_ERROR
                .throwException("line " + line + ", column " + charPositionInLine + ", " + msg);
    }
}