// Generated from XDB.g4 by ANTLR 4.7.1

	package com.dameng.xdb.parser;
	
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.List;
	import java.util.Map;
	import java.util.HashMap;
	
	import com.dameng.xdb.se.model.GObject;
	import com.dameng.xdb.se.model.Link;
	import com.dameng.xdb.se.model.Node;
	import com.dameng.xdb.se.model.PropValue;
	import com.dameng.xdb.stmt.SEExit;
	import com.dameng.xdb.stmt.SELogin;
	import com.dameng.xdb.stmt.SELogout;
	import com.dameng.xdb.stmt.SEPut;
	import com.dameng.xdb.stmt.SEGet;
	import com.dameng.xdb.stmt.SERemove;
	import com.dameng.xdb.stmt.SESet;
	import com.dameng.xdb.stmt.Statement;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class XDBParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		KW_LOGIN=10, KW_LOGOUT=11, KW_EXIT=12, KW_PUT=13, KW_GET=14, KW_SET=15, 
		KW_REMOVE=16, KW_NODE=17, KW_LINK=18, IP=19, NUMBERIC=20, DECIMAL=21, 
		STRING=22, BOOLEAN=23, IDENTIFIER=24, LINE_COMMENT=25, COMMENT=26, WS=27, 
		DIGIT=28, LETTER=29, A=30, B=31, C=32, D=33, E=34, F=35, G=36, H=37, I=38, 
		J=39, K=40, L=41, M=42, N=43, O=44, P=45, Q=46, R=47, S=48, T=49, U=50, 
		V=51, W=52, X=53, Y=54, Z=55, OTHER=56;
	public static final int
		RULE_xdb = 0, RULE_stmt = 1, RULE_se_stmt = 2, RULE_categorys = 3, RULE_category = 4, 
		RULE_properties = 5, RULE_property = 6, RULE_property_key = 7, RULE_property_val = 8, 
		RULE_obj_ids = 9, RULE_obj_id = 10, RULE_host = 11, RULE_port = 12, RULE_identifier = 13, 
		RULE_end = 14, RULE_lt_numberic = 15, RULE_lt_decimal = 16, RULE_lt_string = 17, 
		RULE_lt_boolean = 18;
	public static final String[] ruleNames = {
		"xdb", "stmt", "se_stmt", "categorys", "category", "properties", "property", 
		"property_key", "property_val", "obj_ids", "obj_id", "host", "port", "identifier", 
		"end", "lt_numberic", "lt_decimal", "lt_string", "lt_boolean"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "':'", "'@'", "'/'", "'('", "')'", "'{'", "'}'", "','", "';'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, "KW_LOGIN", 
		"KW_LOGOUT", "KW_EXIT", "KW_PUT", "KW_GET", "KW_SET", "KW_REMOVE", "KW_NODE", 
		"KW_LINK", "IP", "NUMBERIC", "DECIMAL", "STRING", "BOOLEAN", "IDENTIFIER", 
		"LINE_COMMENT", "COMMENT", "WS", "DIGIT", "LETTER", "A", "B", "C", "D", 
		"E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", 
		"S", "T", "U", "V", "W", "X", "Y", "Z", "OTHER"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "XDB.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


		public List<Statement> stmtList = new ArrayList<Statement>();

	public XDBParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class XdbContext extends ParserRuleContext {
		public List<StmtContext> stmt() {
			return getRuleContexts(StmtContext.class);
		}
		public StmtContext stmt(int i) {
			return getRuleContext(StmtContext.class,i);
		}
		public XdbContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_xdb; }
	}

	public final XdbContext xdb() throws RecognitionException {
		XdbContext _localctx = new XdbContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_xdb);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(39); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(38);
				stmt();
				}
				}
				setState(41); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << KW_LOGIN) | (1L << KW_LOGOUT) | (1L << KW_EXIT) | (1L << KW_PUT) | (1L << KW_GET) | (1L << KW_SET) | (1L << KW_REMOVE))) != 0) );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StmtContext extends ParserRuleContext {
		public Se_stmtContext se_stmt;
		public Se_stmtContext se_stmt() {
			return getRuleContext(Se_stmtContext.class,0);
		}
		public StmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stmt; }
	}

	public final StmtContext stmt() throws RecognitionException {
		StmtContext _localctx = new StmtContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(43);
			((StmtContext)_localctx).se_stmt = se_stmt();

					stmtList.add(((StmtContext)_localctx).se_stmt.ret);
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Se_stmtContext extends ParserRuleContext {
		public Statement ret;
		public HostContext host;
		public PortContext port;
		public IdentifierContext user;
		public IdentifierContext password;
		public CategorysContext categorys;
		public PropertiesContext properties;
		public Obj_idContext fnode;
		public Obj_idContext tnode;
		public Obj_idsContext obj_ids;
		public Obj_idContext obj_id;
		public TerminalNode KW_LOGIN() { return getToken(XDBParser.KW_LOGIN, 0); }
		public HostContext host() {
			return getRuleContext(HostContext.class,0);
		}
		public PortContext port() {
			return getRuleContext(PortContext.class,0);
		}
		public EndContext end() {
			return getRuleContext(EndContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode KW_LOGOUT() { return getToken(XDBParser.KW_LOGOUT, 0); }
		public TerminalNode KW_EXIT() { return getToken(XDBParser.KW_EXIT, 0); }
		public TerminalNode KW_PUT() { return getToken(XDBParser.KW_PUT, 0); }
		public TerminalNode KW_NODE() { return getToken(XDBParser.KW_NODE, 0); }
		public CategorysContext categorys() {
			return getRuleContext(CategorysContext.class,0);
		}
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public TerminalNode KW_LINK() { return getToken(XDBParser.KW_LINK, 0); }
		public List<Obj_idContext> obj_id() {
			return getRuleContexts(Obj_idContext.class);
		}
		public Obj_idContext obj_id(int i) {
			return getRuleContext(Obj_idContext.class,i);
		}
		public TerminalNode KW_GET() { return getToken(XDBParser.KW_GET, 0); }
		public Obj_idsContext obj_ids() {
			return getRuleContext(Obj_idsContext.class,0);
		}
		public TerminalNode KW_SET() { return getToken(XDBParser.KW_SET, 0); }
		public TerminalNode KW_REMOVE() { return getToken(XDBParser.KW_REMOVE, 0); }
		public Se_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_se_stmt; }
	}

	public final Se_stmtContext se_stmt() throws RecognitionException {
		Se_stmtContext _localctx = new Se_stmtContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_se_stmt);
		try {
			setState(174);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(46);
				match(KW_LOGIN);
				setState(47);
				((Se_stmtContext)_localctx).host = host();
				setState(48);
				match(T__0);
				setState(49);
				((Se_stmtContext)_localctx).port = port();
				setState(50);
				end();

						((Se_stmtContext)_localctx).ret =  new SELogin(((Se_stmtContext)_localctx).host.ret, ((Se_stmtContext)_localctx).port.ret);
					
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(53);
				match(KW_LOGIN);
				setState(54);
				((Se_stmtContext)_localctx).host = host();
				setState(55);
				match(T__0);
				setState(56);
				((Se_stmtContext)_localctx).port = port();
				setState(57);
				match(T__1);
				setState(58);
				((Se_stmtContext)_localctx).user = identifier();
				setState(59);
				match(T__2);
				setState(60);
				((Se_stmtContext)_localctx).password = identifier();
				setState(61);
				end();

						((Se_stmtContext)_localctx).ret =  new SELogin(((Se_stmtContext)_localctx).host.ret, ((Se_stmtContext)_localctx).port.ret, ((Se_stmtContext)_localctx).user.ret, ((Se_stmtContext)_localctx).password.ret);
					
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(64);
				match(KW_LOGOUT);

						((Se_stmtContext)_localctx).ret =  new SELogout();
					
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(66);
				match(KW_EXIT);

						((Se_stmtContext)_localctx).ret =  new SEExit();
					
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(68);
				match(KW_PUT);
				setState(69);
				match(KW_NODE);
				setState(70);
				match(T__3);
				setState(71);
				((Se_stmtContext)_localctx).categorys = categorys();
				setState(72);
				match(T__4);
				setState(73);
				match(T__5);
				setState(74);
				((Se_stmtContext)_localctx).properties = properties();
				setState(75);
				match(T__6);
				setState(76);
				end();

						Node node = new Node(((Se_stmtContext)_localctx).categorys.ret);
						node.propMap = ((Se_stmtContext)_localctx).properties.ret;
						((Se_stmtContext)_localctx).ret =  new SEPut(true, node);
					
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(79);
				match(KW_PUT);
				setState(80);
				match(KW_LINK);
				setState(81);
				match(T__3);
				setState(82);
				((Se_stmtContext)_localctx).fnode = obj_id();
				setState(83);
				match(T__7);
				setState(84);
				((Se_stmtContext)_localctx).tnode = obj_id();
				setState(85);
				match(T__7);
				setState(86);
				((Se_stmtContext)_localctx).categorys = categorys();
				setState(87);
				match(T__4);
				setState(88);
				match(T__5);
				setState(89);
				((Se_stmtContext)_localctx).properties = properties();
				setState(90);
				match(T__6);
				setState(91);
				end();

						Link link = new Link(((Se_stmtContext)_localctx).categorys.ret);
						link.fnode = ((Se_stmtContext)_localctx).fnode.ret;
						link.tnode = ((Se_stmtContext)_localctx).tnode.ret;
						link.propMap = ((Se_stmtContext)_localctx).properties.ret;
						((Se_stmtContext)_localctx).ret =  new SEPut(false, link);
					
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(94);
				match(KW_GET);
				setState(95);
				match(KW_NODE);
				setState(96);
				match(T__3);
				setState(97);
				((Se_stmtContext)_localctx).obj_ids = obj_ids();
				setState(98);
				match(T__4);
				setState(99);
				end();

						((Se_stmtContext)_localctx).ret =  new SEGet(true, ((Se_stmtContext)_localctx).obj_ids.ret);
					
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(102);
				match(KW_GET);
				setState(103);
				match(KW_LINK);
				setState(104);
				match(T__3);
				setState(105);
				((Se_stmtContext)_localctx).obj_ids = obj_ids();
				setState(106);
				match(T__4);
				setState(107);
				end();

						((Se_stmtContext)_localctx).ret =  new SEGet(false, ((Se_stmtContext)_localctx).obj_ids.ret);
					
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(110);
				match(KW_SET);
				setState(111);
				match(KW_NODE);
				setState(112);
				match(T__3);
				setState(113);
				((Se_stmtContext)_localctx).obj_id = obj_id();
				setState(114);
				match(T__4);
				setState(115);
				match(T__5);
				setState(116);
				((Se_stmtContext)_localctx).properties = properties();
				setState(117);
				match(T__6);
				setState(118);
				end();

						Node node = new Node(((Se_stmtContext)_localctx).obj_id.ret);
						node.propMap = ((Se_stmtContext)_localctx).properties.ret;
						((Se_stmtContext)_localctx).ret =  new SESet(true, node);
					
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(121);
				match(KW_SET);
				setState(122);
				match(KW_NODE);
				setState(123);
				match(T__3);
				setState(124);
				((Se_stmtContext)_localctx).obj_id = obj_id();
				setState(125);
				match(T__7);
				setState(126);
				((Se_stmtContext)_localctx).categorys = categorys();
				setState(127);
				match(T__4);
				setState(128);
				match(T__5);
				setState(129);
				((Se_stmtContext)_localctx).properties = properties();
				setState(130);
				match(T__6);
				setState(131);
				end();

						Node node = new Node(((Se_stmtContext)_localctx).obj_id.ret, ((Se_stmtContext)_localctx).categorys.ret);
						node.propMap = ((Se_stmtContext)_localctx).properties.ret;
						((Se_stmtContext)_localctx).ret =  new SESet(true, node);
					
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(134);
				match(KW_SET);
				setState(135);
				match(KW_LINK);
				setState(136);
				match(T__3);
				setState(137);
				((Se_stmtContext)_localctx).obj_id = obj_id();
				setState(138);
				match(T__4);
				setState(139);
				match(T__5);
				setState(140);
				((Se_stmtContext)_localctx).properties = properties();
				setState(141);
				match(T__6);
				setState(142);
				end();

						Link link = new Link(((Se_stmtContext)_localctx).obj_id.ret);
						link.propMap = ((Se_stmtContext)_localctx).properties.ret;
						((Se_stmtContext)_localctx).ret =  new SESet(false, link);
					
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(145);
				match(KW_SET);
				setState(146);
				match(KW_LINK);
				setState(147);
				match(T__3);
				setState(148);
				((Se_stmtContext)_localctx).obj_id = obj_id();
				setState(149);
				match(T__7);
				setState(150);
				((Se_stmtContext)_localctx).categorys = categorys();
				setState(151);
				match(T__4);
				setState(152);
				match(T__5);
				setState(153);
				((Se_stmtContext)_localctx).properties = properties();
				setState(154);
				match(T__6);
				setState(155);
				end();

						Link link = new Link(((Se_stmtContext)_localctx).obj_id.ret, ((Se_stmtContext)_localctx).categorys.ret);
						link.propMap = ((Se_stmtContext)_localctx).properties.ret;
						((Se_stmtContext)_localctx).ret =  new SESet(false, link);
					
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(158);
				match(KW_REMOVE);
				setState(159);
				match(KW_NODE);
				setState(160);
				match(T__3);
				setState(161);
				((Se_stmtContext)_localctx).obj_ids = obj_ids();
				setState(162);
				match(T__4);
				setState(163);
				end();

						((Se_stmtContext)_localctx).ret =  new SERemove(true, ((Se_stmtContext)_localctx).obj_ids.ret);
					
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(166);
				match(KW_REMOVE);
				setState(167);
				match(KW_LINK);
				setState(168);
				match(T__3);
				setState(169);
				((Se_stmtContext)_localctx).obj_ids = obj_ids();
				setState(170);
				match(T__4);
				setState(171);
				end();

						((Se_stmtContext)_localctx).ret =  new SERemove(false, ((Se_stmtContext)_localctx).obj_ids.ret);
					
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CategorysContext extends ParserRuleContext {
		public String[] ret;
		public CategoryContext category;
		public CategorysContext categorys;
		public CategoryContext category() {
			return getRuleContext(CategoryContext.class,0);
		}
		public CategorysContext categorys() {
			return getRuleContext(CategorysContext.class,0);
		}
		public CategorysContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_categorys; }
	}

	public final CategorysContext categorys() throws RecognitionException {
		CategorysContext _localctx = new CategorysContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_categorys);
		try {
			setState(185);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{

						((CategorysContext)_localctx).ret =  new String[0];
					
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(177);
				((CategorysContext)_localctx).category = category();

						((CategorysContext)_localctx).ret =  new String[] {((CategorysContext)_localctx).category.ret};
					
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(180);
				((CategorysContext)_localctx).category = category();
				setState(181);
				match(T__7);
				setState(182);
				((CategorysContext)_localctx).categorys = categorys();

						String[] cats = new String[1 + ((CategorysContext)_localctx).categorys.ret.length];
						cats[0] = ((CategorysContext)_localctx).category.ret;
						System.arraycopy(((CategorysContext)_localctx).categorys.ret, 0, cats, 1, ((CategorysContext)_localctx).categorys.ret.length);
						((CategorysContext)_localctx).ret =  cats;
					
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CategoryContext extends ParserRuleContext {
		public String ret;
		public Lt_stringContext lt_string;
		public Lt_stringContext lt_string() {
			return getRuleContext(Lt_stringContext.class,0);
		}
		public CategoryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_category; }
	}

	public final CategoryContext category() throws RecognitionException {
		CategoryContext _localctx = new CategoryContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_category);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(187);
			((CategoryContext)_localctx).lt_string = lt_string();

					((CategoryContext)_localctx).ret =  ((CategoryContext)_localctx).lt_string.ret;
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertiesContext extends ParserRuleContext {
		public Map<String, PropValue> ret;
		public PropertyContext property;
		public PropertiesContext properties;
		public PropertyContext property() {
			return getRuleContext(PropertyContext.class,0);
		}
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public PropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_properties; }
	}

	public final PropertiesContext properties() throws RecognitionException {
		PropertiesContext _localctx = new PropertiesContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_properties);

			Map<String, PropValue> propMap = new HashMap<String, PropValue>();

		try {
			setState(199);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(191);
				((PropertiesContext)_localctx).property = property();

						propMap.put(((PropertiesContext)_localctx).property.key, ((PropertiesContext)_localctx).property.val);
					
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(194);
				((PropertiesContext)_localctx).property = property();
				setState(195);
				match(T__7);
				setState(196);
				((PropertiesContext)_localctx).properties = properties();

						propMap.put(((PropertiesContext)_localctx).property.key, ((PropertiesContext)_localctx).property.val);
						propMap.putAll(((PropertiesContext)_localctx).properties.ret);
					
				}
				break;
			}
			_ctx.stop = _input.LT(-1);

				((PropertiesContext)_localctx).ret =  propMap;

		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertyContext extends ParserRuleContext {
		public String key;
		public PropValue val;
		public Property_keyContext property_key;
		public Property_valContext property_val;
		public Property_keyContext property_key() {
			return getRuleContext(Property_keyContext.class,0);
		}
		public Property_valContext property_val() {
			return getRuleContext(Property_valContext.class,0);
		}
		public PropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property; }
	}

	public final PropertyContext property() throws RecognitionException {
		PropertyContext _localctx = new PropertyContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_property);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(201);
			((PropertyContext)_localctx).property_key = property_key();
			setState(202);
			match(T__0);
			setState(203);
			((PropertyContext)_localctx).property_val = property_val();

					((PropertyContext)_localctx).key =  ((PropertyContext)_localctx).property_key.ret;
					((PropertyContext)_localctx).val =  ((PropertyContext)_localctx).property_val.ret;
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Property_keyContext extends ParserRuleContext {
		public String ret;
		public IdentifierContext identifier;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Property_keyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property_key; }
	}

	public final Property_keyContext property_key() throws RecognitionException {
		Property_keyContext _localctx = new Property_keyContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_property_key);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(206);
			((Property_keyContext)_localctx).identifier = identifier();

					((Property_keyContext)_localctx).ret =  ((Property_keyContext)_localctx).identifier.ret;
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Property_valContext extends ParserRuleContext {
		public PropValue ret;
		public Lt_numbericContext lt_numberic;
		public Lt_decimalContext lt_decimal;
		public Lt_stringContext lt_string;
		public Lt_booleanContext lt_boolean;
		public Lt_numbericContext lt_numberic() {
			return getRuleContext(Lt_numbericContext.class,0);
		}
		public Lt_decimalContext lt_decimal() {
			return getRuleContext(Lt_decimalContext.class,0);
		}
		public Lt_stringContext lt_string() {
			return getRuleContext(Lt_stringContext.class,0);
		}
		public Lt_booleanContext lt_boolean() {
			return getRuleContext(Lt_booleanContext.class,0);
		}
		public Property_valContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property_val; }
	}

	public final Property_valContext property_val() throws RecognitionException {
		Property_valContext _localctx = new Property_valContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_property_val);
		try {
			setState(221);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NUMBERIC:
				enterOuterAlt(_localctx, 1);
				{
				setState(209);
				((Property_valContext)_localctx).lt_numberic = lt_numberic();

						((Property_valContext)_localctx).ret =  new PropValue(((Property_valContext)_localctx).lt_numberic.ret, PropValue.TYPE_NUMBERIC);;
					
				}
				break;
			case DECIMAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(212);
				((Property_valContext)_localctx).lt_decimal = lt_decimal();

						 ((Property_valContext)_localctx).ret =  new PropValue(((Property_valContext)_localctx).lt_decimal.ret, PropValue.TYPE_DECIMAL);
					
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 3);
				{
				setState(215);
				((Property_valContext)_localctx).lt_string = lt_string();

						 ((Property_valContext)_localctx).ret =  new PropValue(((Property_valContext)_localctx).lt_string.ret, PropValue.TYPE_STRING);
					
				}
				break;
			case BOOLEAN:
				enterOuterAlt(_localctx, 4);
				{
				setState(218);
				((Property_valContext)_localctx).lt_boolean = lt_boolean();

						((Property_valContext)_localctx).ret =  new PropValue(((Property_valContext)_localctx).lt_boolean.ret, PropValue.TYPE_BOOLEAN);
					
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Obj_idsContext extends ParserRuleContext {
		public int[] ret;
		public Obj_idContext obj_id;
		public Obj_idsContext obj_ids;
		public Obj_idContext obj_id() {
			return getRuleContext(Obj_idContext.class,0);
		}
		public Obj_idsContext obj_ids() {
			return getRuleContext(Obj_idsContext.class,0);
		}
		public Obj_idsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_obj_ids; }
	}

	public final Obj_idsContext obj_ids() throws RecognitionException {
		Obj_idsContext _localctx = new Obj_idsContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_obj_ids);
		try {
			setState(231);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(223);
				((Obj_idsContext)_localctx).obj_id = obj_id();

						((Obj_idsContext)_localctx).ret =  new int[] {((Obj_idsContext)_localctx).obj_id.ret};
					
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(226);
				((Obj_idsContext)_localctx).obj_id = obj_id();
				setState(227);
				match(T__7);
				setState(228);
				((Obj_idsContext)_localctx).obj_ids = obj_ids();

					int[] ids = new int[1 + ((Obj_idsContext)_localctx).obj_ids.ret.length];
					ids[0] = ((Obj_idsContext)_localctx).obj_id.ret;
					System.arraycopy(((Obj_idsContext)_localctx).obj_ids.ret, 0, ids, 1, ((Obj_idsContext)_localctx).obj_ids.ret.length);
					((Obj_idsContext)_localctx).ret =  ids;

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Obj_idContext extends ParserRuleContext {
		public int ret;
		public Lt_numbericContext lt_numberic;
		public Lt_numbericContext lt_numberic() {
			return getRuleContext(Lt_numbericContext.class,0);
		}
		public Obj_idContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_obj_id; }
	}

	public final Obj_idContext obj_id() throws RecognitionException {
		Obj_idContext _localctx = new Obj_idContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_obj_id);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(233);
			((Obj_idContext)_localctx).lt_numberic = lt_numberic();

					((Obj_idContext)_localctx).ret =  ((Obj_idContext)_localctx).lt_numberic.ret.intValue();
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HostContext extends ParserRuleContext {
		public String ret;
		public Token IP;
		public Token IDENTIFIER;
		public TerminalNode IP() { return getToken(XDBParser.IP, 0); }
		public TerminalNode IDENTIFIER() { return getToken(XDBParser.IDENTIFIER, 0); }
		public HostContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_host; }
	}

	public final HostContext host() throws RecognitionException {
		HostContext _localctx = new HostContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_host);
		try {
			setState(240);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IP:
				enterOuterAlt(_localctx, 1);
				{
				setState(236);
				((HostContext)_localctx).IP = match(IP);

						((HostContext)_localctx).ret =  ((HostContext)_localctx).IP.getText();
					
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(238);
				((HostContext)_localctx).IDENTIFIER = match(IDENTIFIER);

						((HostContext)_localctx).ret =  ((HostContext)_localctx).IDENTIFIER.getText();
					
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PortContext extends ParserRuleContext {
		public int ret;
		public Lt_numbericContext lt_numberic;
		public Lt_numbericContext lt_numberic() {
			return getRuleContext(Lt_numbericContext.class,0);
		}
		public PortContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_port; }
	}

	public final PortContext port() throws RecognitionException {
		PortContext _localctx = new PortContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_port);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(242);
			((PortContext)_localctx).lt_numberic = lt_numberic();

					((PortContext)_localctx).ret =  ((PortContext)_localctx).lt_numberic.ret.intValue();
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public String ret;
		public Token IDENTIFIER;
		public TerminalNode IDENTIFIER() { return getToken(XDBParser.IDENTIFIER, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(245);
			((IdentifierContext)_localctx).IDENTIFIER = match(IDENTIFIER);

					String text = ((IdentifierContext)_localctx).IDENTIFIER.getText();
					if (text.startsWith("\""))
					{
						text = text.substring(1, text.length() - 1);
					}
					((IdentifierContext)_localctx).ret =  text;
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EndContext extends ParserRuleContext {
		public EndContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_end; }
	}

	public final EndContext end() throws RecognitionException {
		EndContext _localctx = new EndContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_end);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__8) {
				{
				setState(248);
				match(T__8);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Lt_numbericContext extends ParserRuleContext {
		public Long ret;
		public Token NUMBERIC;
		public TerminalNode NUMBERIC() { return getToken(XDBParser.NUMBERIC, 0); }
		public Lt_numbericContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lt_numberic; }
	}

	public final Lt_numbericContext lt_numberic() throws RecognitionException {
		Lt_numbericContext _localctx = new Lt_numbericContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_lt_numberic);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(251);
			((Lt_numbericContext)_localctx).NUMBERIC = match(NUMBERIC);

					((Lt_numbericContext)_localctx).ret =  Long.valueOf(((Lt_numbericContext)_localctx).NUMBERIC.getText());
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Lt_decimalContext extends ParserRuleContext {
		public Double ret;
		public Token DECIMAL;
		public TerminalNode DECIMAL() { return getToken(XDBParser.DECIMAL, 0); }
		public Lt_decimalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lt_decimal; }
	}

	public final Lt_decimalContext lt_decimal() throws RecognitionException {
		Lt_decimalContext _localctx = new Lt_decimalContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_lt_decimal);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(254);
			((Lt_decimalContext)_localctx).DECIMAL = match(DECIMAL);

					((Lt_decimalContext)_localctx).ret =  Double.valueOf(((Lt_decimalContext)_localctx).DECIMAL.getText());
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Lt_stringContext extends ParserRuleContext {
		public String ret;
		public Token STRING;
		public TerminalNode STRING() { return getToken(XDBParser.STRING, 0); }
		public Lt_stringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lt_string; }
	}

	public final Lt_stringContext lt_string() throws RecognitionException {
		Lt_stringContext _localctx = new Lt_stringContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_lt_string);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(257);
			((Lt_stringContext)_localctx).STRING = match(STRING);

					String text = ((Lt_stringContext)_localctx).STRING.getText();
					text = text.substring(1, text.length() - 1);
					text = text.replaceAll("\\\\'", "'");
					((Lt_stringContext)_localctx).ret =  text;
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Lt_booleanContext extends ParserRuleContext {
		public Boolean ret;
		public Token BOOLEAN;
		public TerminalNode BOOLEAN() { return getToken(XDBParser.BOOLEAN, 0); }
		public Lt_booleanContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lt_boolean; }
	}

	public final Lt_booleanContext lt_boolean() throws RecognitionException {
		Lt_booleanContext _localctx = new Lt_booleanContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_lt_boolean);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(260);
			((Lt_booleanContext)_localctx).BOOLEAN = match(BOOLEAN);

					((Lt_booleanContext)_localctx).ret =  Boolean.valueOf(((Lt_booleanContext)_localctx).BOOLEAN.getText());
				
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3:\u010a\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\3\2\6\2*\n\2\r\2\16\2+\3\3\3\3\3\3\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\5\4\u00b1\n\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u00bc\n"+
		"\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u00ca\n\7\3\b\3"+
		"\b\3\b\3\b\3\b\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\5\n\u00e0\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u00ea"+
		"\n\13\3\f\3\f\3\f\3\r\3\r\3\r\3\r\5\r\u00f3\n\r\3\16\3\16\3\16\3\17\3"+
		"\17\3\17\3\20\5\20\u00fc\n\20\3\21\3\21\3\21\3\22\3\22\3\22\3\23\3\23"+
		"\3\23\3\24\3\24\3\24\3\24\2\2\25\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36"+
		" \"$&\2\2\2\u010e\2)\3\2\2\2\4-\3\2\2\2\6\u00b0\3\2\2\2\b\u00bb\3\2\2"+
		"\2\n\u00bd\3\2\2\2\f\u00c9\3\2\2\2\16\u00cb\3\2\2\2\20\u00d0\3\2\2\2\22"+
		"\u00df\3\2\2\2\24\u00e9\3\2\2\2\26\u00eb\3\2\2\2\30\u00f2\3\2\2\2\32\u00f4"+
		"\3\2\2\2\34\u00f7\3\2\2\2\36\u00fb\3\2\2\2 \u00fd\3\2\2\2\"\u0100\3\2"+
		"\2\2$\u0103\3\2\2\2&\u0106\3\2\2\2(*\5\4\3\2)(\3\2\2\2*+\3\2\2\2+)\3\2"+
		"\2\2+,\3\2\2\2,\3\3\2\2\2-.\5\6\4\2./\b\3\1\2/\5\3\2\2\2\60\61\7\f\2\2"+
		"\61\62\5\30\r\2\62\63\7\3\2\2\63\64\5\32\16\2\64\65\5\36\20\2\65\66\b"+
		"\4\1\2\66\u00b1\3\2\2\2\678\7\f\2\289\5\30\r\29:\7\3\2\2:;\5\32\16\2;"+
		"<\7\4\2\2<=\5\34\17\2=>\7\5\2\2>?\5\34\17\2?@\5\36\20\2@A\b\4\1\2A\u00b1"+
		"\3\2\2\2BC\7\r\2\2C\u00b1\b\4\1\2DE\7\16\2\2E\u00b1\b\4\1\2FG\7\17\2\2"+
		"GH\7\23\2\2HI\7\6\2\2IJ\5\b\5\2JK\7\7\2\2KL\7\b\2\2LM\5\f\7\2MN\7\t\2"+
		"\2NO\5\36\20\2OP\b\4\1\2P\u00b1\3\2\2\2QR\7\17\2\2RS\7\24\2\2ST\7\6\2"+
		"\2TU\5\26\f\2UV\7\n\2\2VW\5\26\f\2WX\7\n\2\2XY\5\b\5\2YZ\7\7\2\2Z[\7\b"+
		"\2\2[\\\5\f\7\2\\]\7\t\2\2]^\5\36\20\2^_\b\4\1\2_\u00b1\3\2\2\2`a\7\20"+
		"\2\2ab\7\23\2\2bc\7\6\2\2cd\5\24\13\2de\7\7\2\2ef\5\36\20\2fg\b\4\1\2"+
		"g\u00b1\3\2\2\2hi\7\20\2\2ij\7\24\2\2jk\7\6\2\2kl\5\24\13\2lm\7\7\2\2"+
		"mn\5\36\20\2no\b\4\1\2o\u00b1\3\2\2\2pq\7\21\2\2qr\7\23\2\2rs\7\6\2\2"+
		"st\5\26\f\2tu\7\7\2\2uv\7\b\2\2vw\5\f\7\2wx\7\t\2\2xy\5\36\20\2yz\b\4"+
		"\1\2z\u00b1\3\2\2\2{|\7\21\2\2|}\7\23\2\2}~\7\6\2\2~\177\5\26\f\2\177"+
		"\u0080\7\n\2\2\u0080\u0081\5\b\5\2\u0081\u0082\7\7\2\2\u0082\u0083\7\b"+
		"\2\2\u0083\u0084\5\f\7\2\u0084\u0085\7\t\2\2\u0085\u0086\5\36\20\2\u0086"+
		"\u0087\b\4\1\2\u0087\u00b1\3\2\2\2\u0088\u0089\7\21\2\2\u0089\u008a\7"+
		"\24\2\2\u008a\u008b\7\6\2\2\u008b\u008c\5\26\f\2\u008c\u008d\7\7\2\2\u008d"+
		"\u008e\7\b\2\2\u008e\u008f\5\f\7\2\u008f\u0090\7\t\2\2\u0090\u0091\5\36"+
		"\20\2\u0091\u0092\b\4\1\2\u0092\u00b1\3\2\2\2\u0093\u0094\7\21\2\2\u0094"+
		"\u0095\7\24\2\2\u0095\u0096\7\6\2\2\u0096\u0097\5\26\f\2\u0097\u0098\7"+
		"\n\2\2\u0098\u0099\5\b\5\2\u0099\u009a\7\7\2\2\u009a\u009b\7\b\2\2\u009b"+
		"\u009c\5\f\7\2\u009c\u009d\7\t\2\2\u009d\u009e\5\36\20\2\u009e\u009f\b"+
		"\4\1\2\u009f\u00b1\3\2\2\2\u00a0\u00a1\7\22\2\2\u00a1\u00a2\7\23\2\2\u00a2"+
		"\u00a3\7\6\2\2\u00a3\u00a4\5\24\13\2\u00a4\u00a5\7\7\2\2\u00a5\u00a6\5"+
		"\36\20\2\u00a6\u00a7\b\4\1\2\u00a7\u00b1\3\2\2\2\u00a8\u00a9\7\22\2\2"+
		"\u00a9\u00aa\7\24\2\2\u00aa\u00ab\7\6\2\2\u00ab\u00ac\5\24\13\2\u00ac"+
		"\u00ad\7\7\2\2\u00ad\u00ae\5\36\20\2\u00ae\u00af\b\4\1\2\u00af\u00b1\3"+
		"\2\2\2\u00b0\60\3\2\2\2\u00b0\67\3\2\2\2\u00b0B\3\2\2\2\u00b0D\3\2\2\2"+
		"\u00b0F\3\2\2\2\u00b0Q\3\2\2\2\u00b0`\3\2\2\2\u00b0h\3\2\2\2\u00b0p\3"+
		"\2\2\2\u00b0{\3\2\2\2\u00b0\u0088\3\2\2\2\u00b0\u0093\3\2\2\2\u00b0\u00a0"+
		"\3\2\2\2\u00b0\u00a8\3\2\2\2\u00b1\7\3\2\2\2\u00b2\u00bc\b\5\1\2\u00b3"+
		"\u00b4\5\n\6\2\u00b4\u00b5\b\5\1\2\u00b5\u00bc\3\2\2\2\u00b6\u00b7\5\n"+
		"\6\2\u00b7\u00b8\7\n\2\2\u00b8\u00b9\5\b\5\2\u00b9\u00ba\b\5\1\2\u00ba"+
		"\u00bc\3\2\2\2\u00bb\u00b2\3\2\2\2\u00bb\u00b3\3\2\2\2\u00bb\u00b6\3\2"+
		"\2\2\u00bc\t\3\2\2\2\u00bd\u00be\5$\23\2\u00be\u00bf\b\6\1\2\u00bf\13"+
		"\3\2\2\2\u00c0\u00ca\b\7\1\2\u00c1\u00c2\5\16\b\2\u00c2\u00c3\b\7\1\2"+
		"\u00c3\u00ca\3\2\2\2\u00c4\u00c5\5\16\b\2\u00c5\u00c6\7\n\2\2\u00c6\u00c7"+
		"\5\f\7\2\u00c7\u00c8\b\7\1\2\u00c8\u00ca\3\2\2\2\u00c9\u00c0\3\2\2\2\u00c9"+
		"\u00c1\3\2\2\2\u00c9\u00c4\3\2\2\2\u00ca\r\3\2\2\2\u00cb\u00cc\5\20\t"+
		"\2\u00cc\u00cd\7\3\2\2\u00cd\u00ce\5\22\n\2\u00ce\u00cf\b\b\1\2\u00cf"+
		"\17\3\2\2\2\u00d0\u00d1\5\34\17\2\u00d1\u00d2\b\t\1\2\u00d2\21\3\2\2\2"+
		"\u00d3\u00d4\5 \21\2\u00d4\u00d5\b\n\1\2\u00d5\u00e0\3\2\2\2\u00d6\u00d7"+
		"\5\"\22\2\u00d7\u00d8\b\n\1\2\u00d8\u00e0\3\2\2\2\u00d9\u00da\5$\23\2"+
		"\u00da\u00db\b\n\1\2\u00db\u00e0\3\2\2\2\u00dc\u00dd\5&\24\2\u00dd\u00de"+
		"\b\n\1\2\u00de\u00e0\3\2\2\2\u00df\u00d3\3\2\2\2\u00df\u00d6\3\2\2\2\u00df"+
		"\u00d9\3\2\2\2\u00df\u00dc\3\2\2\2\u00e0\23\3\2\2\2\u00e1\u00e2\5\26\f"+
		"\2\u00e2\u00e3\b\13\1\2\u00e3\u00ea\3\2\2\2\u00e4\u00e5\5\26\f\2\u00e5"+
		"\u00e6\7\n\2\2\u00e6\u00e7\5\24\13\2\u00e7\u00e8\b\13\1\2\u00e8\u00ea"+
		"\3\2\2\2\u00e9\u00e1\3\2\2\2\u00e9\u00e4\3\2\2\2\u00ea\25\3\2\2\2\u00eb"+
		"\u00ec\5 \21\2\u00ec\u00ed\b\f\1\2\u00ed\27\3\2\2\2\u00ee\u00ef\7\25\2"+
		"\2\u00ef\u00f3\b\r\1\2\u00f0\u00f1\7\32\2\2\u00f1\u00f3\b\r\1\2\u00f2"+
		"\u00ee\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f3\31\3\2\2\2\u00f4\u00f5\5 \21"+
		"\2\u00f5\u00f6\b\16\1\2\u00f6\33\3\2\2\2\u00f7\u00f8\7\32\2\2\u00f8\u00f9"+
		"\b\17\1\2\u00f9\35\3\2\2\2\u00fa\u00fc\7\13\2\2\u00fb\u00fa\3\2\2\2\u00fb"+
		"\u00fc\3\2\2\2\u00fc\37\3\2\2\2\u00fd\u00fe\7\26\2\2\u00fe\u00ff\b\21"+
		"\1\2\u00ff!\3\2\2\2\u0100\u0101\7\27\2\2\u0101\u0102\b\22\1\2\u0102#\3"+
		"\2\2\2\u0103\u0104\7\30\2\2\u0104\u0105\b\23\1\2\u0105%\3\2\2\2\u0106"+
		"\u0107\7\31\2\2\u0107\u0108\b\24\1\2\u0108\'\3\2\2\2\n+\u00b0\u00bb\u00c9"+
		"\u00df\u00e9\u00f2\u00fb";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}