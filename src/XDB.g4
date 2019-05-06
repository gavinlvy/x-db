/***********************************
* grammar Name;
* options {...}
* import ...;
* tokens {...}
* @actionName {...}
* <<rule1>>
* ...
* <<rule2>>
* 
* 
* access-modifier rule-name[<<arguments>>] returns [<<return-values>>]
*   <<throws-spec>>
*   <<options-spec>>
*   <<rule-attribute-scopes>>
*   <<rule-actions>>
*************************************
* grammer Hello;
* 
* options {
*     language=Java;
*     tokenVocab=HelloLexer;
* }
* 
* @header {
*     package com.laudandjolynn.antlr;
*     import java.util.Set;
*     import java.util.HashSet;
* }
* 
* @member {
*     private int count;
*     public int getCount() {
*         return count;
*     }
* }
*************************************/
grammar XDB;

options {
	language = Java;
}

@lexer::header {
	package com.dameng.xdb.parser;
}

@parser::header {
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
	import com.dameng.xdb.stmt.SEShow;
	import com.dameng.xdb.stmt.SEHelp;
	import com.dameng.xdb.stmt.Statement;
}

@parser::members {
	public List<Statement> stmtList = new ArrayList<Statement>();
}

/*=========================PARSER_RULE=========================*/
xdb
:
	stmt+
;

stmt
:
	se_stmt
	{
		stmtList.add($se_stmt.ret);
	}

;

se_stmt returns [Statement ret]
:
	KW_LOGIN host ':' port end
	{
		$ret = new SELogin($host.ret, $port.ret);
	}

	| KW_LOGIN host ':' port '@' user = identifier '/' password = identifier end
	{
		$ret = new SELogin($host.ret, $port.ret, $user.ret, $password.ret);
	}

	| KW_LOGOUT
	{
		$ret = new SELogout();
	}

	| KW_EXIT
	{
		$ret = new SEExit();
	}

	| KW_HELP
	{
		$ret = new SEHelp();
	}

	| KW_PUT KW_NODE '(' categorys ')' '{' properties '}' end
	{
		Node node = new Node($categorys.ret);
		node.propMap = $properties.ret;
		$ret = new SEPut(true, node);
	}

	| KW_PUT KW_LINK '(' fnode = obj_id ',' tnode = obj_id ',' categorys ')' '{'
	properties '}' end
	{
		Link link = new Link($categorys.ret);
		link.fnode = $fnode.ret;
		link.tnode = $tnode.ret;
		link.propMap = $properties.ret;
		$ret = new SEPut(false, link);
	}

	| KW_GET KW_NODE '(' obj_ids ')' end //# getNode
	{
		$ret = new SEGet(true, $obj_ids.ret);
	}

	| KW_GET KW_LINK '(' obj_ids ')' end //# getLink
	{
		$ret = new SEGet(false, $obj_ids.ret);
	}

	| KW_SET KW_NODE '(' obj_id ')' '{' properties '}' end
	{
		Node node = new Node($obj_id.ret);
		node.propMap = $properties.ret;
		$ret = new SESet(true, node);
	}

	| KW_SET KW_NODE '(' obj_id ',' categorys ')' '{' properties '}' end
	{
		Node node = new Node($obj_id.ret, $categorys.ret);
		node.propMap = $properties.ret;
		$ret = new SESet(true, node);
	}

	| KW_SET KW_LINK '(' obj_id ')' '{' properties '}' end
	{
		Link link = new Link($obj_id.ret);
		link.propMap = $properties.ret;
		$ret = new SESet(false, link);
	}

	| KW_SET KW_LINK '(' obj_id ',' categorys ')' '{' properties '}' end
	{
		Link link = new Link($obj_id.ret, $categorys.ret);
		link.propMap = $properties.ret;
		$ret = new SESet(false, link);
	}

	| KW_REMOVE KW_NODE '(' obj_ids ')' end
	{
		$ret = new SERemove(true, $obj_ids.ret);
	}

	| KW_REMOVE KW_LINK '(' obj_ids ')' end
	{
		$ret = new SERemove(false, $obj_ids.ret);
	}

	| KW_SHOW KW_NODE KW_TOP lt_numberic end
	{
		$ret = new SEShow(true, $lt_numberic.ret.intValue());
	}

	| KW_SHOW KW_LINK KW_TOP lt_numberic end
	{
		$ret = new SEShow(false, $lt_numberic.ret.intValue());
	}

;

categorys returns [String[] ret]
:
	< empty >
	{
		$ret = new String[0];
	}

	| category
	{
		$ret = new String[] {$category.ret};
	}

	| category ',' categorys
	{
		String[] cats = new String[1 + $categorys.ret.length];
		cats[0] = $category.ret;
		System.arraycopy($categorys.ret, 0, cats, 1, $categorys.ret.length);
		$ret = cats;
	}

;

category returns [String ret]
:
	lt_string
	{
		$ret = $lt_string.ret;
	}

;

properties returns [Map<String, PropValue> ret] @init {
	Map<String, PropValue> propMap = new HashMap<String, PropValue>();
}
@after {
	$ret = propMap;
}
:
	< empty >
	{}

	| property
	{
		propMap.put($property.key, $property.val);
	}

	| property ',' properties
	{
		propMap.put($property.key, $property.val);
		propMap.putAll($properties.ret);
	}

;

property returns [String key, PropValue val]
:
	property_key ':' property_val
	{
		$key = $property_key.ret;
		$val = $property_val.ret;
	}

;

property_key returns [String ret]
:
	identifier
	{
		$ret = $identifier.ret;
	}

;

property_val returns [PropValue ret]
:
	lt_numberic
	{
		$ret = new PropValue($lt_numberic.ret, PropValue.TYPE_NUMBERIC);;
	}

	| lt_decimal
	{
		 $ret = new PropValue($lt_decimal.ret, PropValue.TYPE_DECIMAL);
	}

	| lt_string
	{
		 $ret = new PropValue($lt_string.ret, PropValue.TYPE_STRING);
	}

	| lt_boolean
	{
		$ret = new PropValue($lt_boolean.ret, PropValue.TYPE_BOOLEAN);
	}

;

obj_ids returns [int[] ret]
:
	obj_id
	{
		$ret = new int[] {$obj_id.ret};
	}

	| obj_id ',' obj_ids
	{
	int[] ids = new int[1 + $obj_ids.ret.length];
	ids[0] = $obj_id.ret;
	System.arraycopy($obj_ids.ret, 0, ids, 1, $obj_ids.ret.length);
	$ret = ids;
}

;

obj_id returns [int ret]
:
	lt_numberic
	{
		$ret = $lt_numberic.ret.intValue();
	}

;

host returns [String ret]
:
	IP
	{
		$ret = $IP.getText();
	}

	| IDENTIFIER
	{
		$ret = $IDENTIFIER.getText();
	}

;

port returns [int ret]
:
	lt_numberic
	{
		$ret = $lt_numberic.ret.intValue();
	}

;

identifier returns [String ret]
:
	IDENTIFIER
	{
		String text = $IDENTIFIER.getText();
		if (text.startsWith("\""))
		{
			text = text.substring(1, text.length() - 1);
		}
		$ret = text;
	}

;

end
:
	';'?
;

lt_numberic returns [Long ret]
:
	NUMBERIC
	{
		$ret = Long.valueOf($NUMBERIC.getText());
	}

;

lt_decimal returns [Double ret]
:
	DECIMAL
	{
		$ret = Double.valueOf($DECIMAL.getText());
	}

;

lt_string returns [String ret]
:
	STRING
	{
		String text = $STRING.getText();
		text = text.substring(1, text.length() - 1);
		text = text.replaceAll("\\\\'", "'");
		$ret = text;
	}

;

lt_boolean returns [Boolean ret]
:
	BOOLEAN
	{
		$ret = Boolean.valueOf($BOOLEAN.getText());
	}

;

/*=========================LEXER_RULE=========================*/
KW_HELP
:
	H E L P
;

KW_LOGIN
:
	L O G I N
;

KW_LOGOUT
:
	L O G O U T
;

KW_EXIT
:
	E X I T
;

KW_PUT
:
	P U T
;

KW_GET
:
	G E T
;

KW_SET
:
	S E T
;

KW_REMOVE
:
	R E M O V E
;

KW_SHOW
:
	S H O W
;

KW_NODE
:
	N O D E
;

KW_LINK
:
	L I N K
;

KW_TOP
:
	T O P
;

/*---------------*/
IP
:
	DIGIT+ '.' DIGIT+ '.' DIGIT+ '.' DIGIT+
;

NUMBERIC
:
	DIGIT+
	| '-' DIGIT+ //负整数

;

DECIMAL
:
	DIGIT* '.' DIGIT+ //正浮点数

	| '-' DIGIT* '.' DIGIT+ //负浮点数

;

STRING
:
	'\''
	(
		'\\\''
		| '\\\\'
		| .
	)*? '\''
;

BOOLEAN
:
	T R U E
	| F A L S E
;

IDENTIFIER
:
	LETTER
	(
		LETTER
		| DIGIT
	)*
	| '"' LETTER
	(
		LETTER
		| DIGIT
	)* '"'
;

LINE_COMMENT
:
	'//' .*? '\r'? '\n' -> skip
; // Match"//" stuff '\n'

COMMENT
:
	'/*' .*? '*/' -> skip
; // Match "/*" stuff "*/"

WS
:
	[ \t\r\n] -> skip
;

DIGIT
:
	[0-9]
;

LETTER
:
	[_A-Za-z#$]
;

A
:
	[aA]
;

B
:
	[bB]
;

C
:
	[cC]
;

D
:
	[dD]
;

E
:
	[eE]
;

F
:
	[fF]
;

G
:
	[gG]
;

H
:
	[hH]
;

I
:
	[iI]
;

J
:
	[jJ]
;

K
:
	[kK]
;

L
:
	[lL]
;

M
:
	[mM]
;

N
:
	[nN]
;

O
:
	[oO]
;

P
:
	[pP]
;

Q
:
	[qQ]
;

R
:
	[rR]
;

S
:
	[sS]
;

T
:
	[tT]
;

U
:
	[uU]
;

V
:
	[vV]
;

W
:
	[wW]
;

X
:
	[xX]
;

Y
:
	[yY]
;

Z
:
	[zZ]
;

OTHER
:
	.
;



