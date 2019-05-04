//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "TurtleParser.h"
#include <iostream>
#include <sstream>
#include <string.h>
extern char* columns;
using namespace std;
// Constructor
TurtleParser::Lexer::Lexer(istream& in) :in(in), putBack(Eof), line(1), readBufferStart(0), readBufferEnd(0){}
// Destructor
TurtleParser::Lexer::~Lexer(){}
// Read new characters
bool TurtleParser::Lexer::doRead(char& c)
{
	while (in) {
		readBufferStart = readBuffer;
		in.read(readBuffer, readBufferSize); //文件流istream读
		/*
		 * gcount() 方法返回最后一个非格式化的抽取方法读取的字符数。
		 * 这意味着字符由get(),getline()、ignore()、或read()方法读取的，不是由抽取操作符( >> )读取的，
		 * 抽取操作符对输入进行格式化，使之与特定的数据类型匹配*/
		if (!in.gcount())
			return false;
		readBufferEnd = readBufferStart + in.gcount();

		if (readBufferStart < readBufferEnd) {
			c = *(readBufferStart++); //c 为起始的字符
			return true;
		}
	}
	return false;
}
//---------------------------------------------------------------------------
static bool issep(char c) {
	return (c == ' ') || (c == '\t') || (c == '\n') || (c == '\r') || (c == '[')
			|| (c == ']') || (c == '(') || (c == ')') || (c == ',')
			|| (c == ';') || (c == ':') || (c == '.');
}
// Lex a number,一个数字，用token字符串保存起来
TurtleParser::Lexer::Token TurtleParser::Lexer::lexNumber(std::string& token,char c)
{
	token.resize(0);
	while (true) {
		// Sign?
		if ((c == '+') || (c == '-')) {
			token += c;
			if (!read(c))
				break;
		}

		// First number block
		if (c != '.') {
			if ((c < '0') || (c > '9'))
				break;
			while ((c >= '0') && (c <= '9')) {
				token += c;
				if (!read(c))
					return Integer; //c为空
			}
			if (issep(c)) {
				unread();
				return Integer;
			}
		}

		// Dot?
		if (c == '.') {
			token += c;
			if (!read(c))
				break;
			// Second number block
			while ((c >= '0') && (c <= '9')) {
				token += c;
				if (!read(c))
					return Decimal;
			}
			if (issep(c)) {
				unread();
				return Decimal; //带小数点的数值全部用高精度表示
			}
		}

		// Exponent
		if ((c != 'e') && (c != 'E'))
			break;
		token += c;
		if (!read(c))
			break;
		if ((c == '-') || (c == '+')) {
			token += c;
			if (!read(c))
				break;
		}
		if ((c < '0') || (c > '9'))
			break;
		while ((c >= '0') && (c <= '9')) {
			token += c;
			if (!read(c))
				return Double;
		}
		if (issep(c)) {
			unread();
			return Double;
		}
		break;
	}
	cerr << "lexer error in line " << line << ": invalid number " << token << c
			<< endl;
	throw Exception();
}
// Parse a hex code
unsigned TurtleParser::Lexer::lexHexCode(unsigned len)
{
	unsigned result = 0;
	for (unsigned index = 0;; index++) {
		// Done?
		if (index == len)
			return result;

		// Read the next char
		char c;
		if (!read(c))
			break;

		// Interpret it
		if ((c >= '0') && (c <= '9'))
			result = (result << 4) | (c - '0');
		else if ((c >= 'A') && (c <= 'F'))
			result = (result << 4) | (c - 'A' + 10);
		else if ((c >= 'a') && (c <= 'f'))
			result = (result << 4) | (c - 'a' + 10);
		else
			break;
	}
	cerr << "lexer error in line " << line << ": invalid unicode escape"
			<< endl;
	throw Exception();
}
// Encode a unicode character as utf8
static string encodeUtf8(unsigned code)
{
	string result;
	if (code && (code < 0x80)) {
		result += static_cast<char>(code);
	} else if (code < 0x800) {
		result += static_cast<char>(0xc0 | (0x1f & (code >> 6)));
		result += static_cast<char>(0x80 | (0x3f & code));
	} else {
		result += static_cast<char>(0xe0 | (0x0f & (code >> 12)));
		result += static_cast<char>(0x80 | (0x3f & (code >> 6)));
		result += static_cast<char>(0x80 | (0x3f & code));
	}
	return result;
}
//Lex an escape sequence, \ already consumed
//replace the char '\n' in String or URI with '\t'
void TurtleParser::Lexer::lexEscape(std::string& token)
{
	while (true) {
		char c;
		if (!read(c))
			break;
		// Standard escapes?
		if (c == 't') {
			token += '\t';
			return;
		}
		if (c == 'n') {
			//token += '\n';
			token += '\t';
			return;
		}
		if (c == 'r') {
			token += '\r';
			return;
		}
		if (c == '\"') {
			token += '\"';
			return;
		}
		if (c == '>') {
			token += '>';
			return;
		}
		if (c == '\\') {
			token += '\\';
			return;
		}

		// Unicode sequences?
		if (c == 'u') {
			unsigned code = lexHexCode(4);
			token += encodeUtf8(code);
			return;
		}
		if (c == 'U') {
			unsigned code = lexHexCode(8);
			token += encodeUtf8(code);
			return;
		}
		// Invalid escape
		break;
	}
	cerr << "lexer error in line " << line << ": invalid escape sequence"
			<< endl;
	throw Exception();
}
// Lex a long string, first """ already consumed replace the char '\n' with '\t'
TurtleParser::Lexer::Token TurtleParser::Lexer::lexLongString(std::string& token)
{
	char c;
	while (read(c)) {
		if (c == '\"') {
			if (!read(c))
				break;
			if (c != '\"') {
				token += '\"';
				continue;
			}
			if (!read(c))
				break;
			if (c != '\"') {
				token += "\"\"";
				continue;
			}
			return String;
		}
		if (c == '\\') {
			lexEscape(token);
		} else {
			if (c == '\n' || (c == '\r')) {
				line++;
				token += '\t';
			} else
				token += c;
		}
	}
	cerr << "lexer error in line " << line << ": invalid string" << endl;
	throw Exception();
}
// Lex a string
TurtleParser::Lexer::Token TurtleParser::Lexer::lexString(std::string& token,char c)
{
	token.resize(0);
	// Check the next character
	if (!read(c)) {
		cerr << "lexer error in line " << line << ": invalid string" << endl;
		throw Exception();
	}

	// Another quote?
	if (c == '\"') {
		if (!read(c))
			return String;
		if (c == '\"')
			return lexLongString(token);
		unread();
		return String;
	}

	// Process normally
	while (true) {
		if (c == '\"')
			return String;
		if (c == '\\') {
			lexEscape(token);
		} else {
			if (c == '\n' || (c == '\r')) {
				line++;
				token += '\t';
			} else
				token += c;
		}
		if (!read(c)) {
			cerr << "lexer error in line " << line << ": invalid string"
					<< endl;
			throw Exception();
		}
	}
}
// Lex a URI
TurtleParser::Lexer::Token TurtleParser::Lexer::lexURI(std::string& token,char c)
{
	token.resize(0);
	// Check the next character
	if (!read(c)) {
		cerr << "lexer error in line " << line << ": invalid URI" << endl;
		throw Exception();
	}

	// Process normally
	while (true) {
		if (c == '>')
			return URI;
		if (c == '\\') {
			lexEscape(token);
		} else {
			if (c == '\n' || (c == '\r')) {
				line++;
				token += '\t';
			} else
				token += c;
		}
		if (!read(c)) {
			cerr << "lexer error in line " << line << ": invalid URI" << endl;
			throw Exception();
		}
	}
}
// Get the next token
TurtleParser::Lexer::Token TurtleParser::Lexer::next(std::string& token)
{
	// Do we already have one?
	if (putBack != Eof) {
		Token result = putBack;
		token = putBackValue;
		putBack = Eof;
		return result;
	}

	// Read more
	char c;
	while (read(c)) {
		switch (c) {
		case ' ':
		case '\t':
		case '\r':
			continue;
		case '\n':
			line++;
			continue;
		case '#':
			while (read(c))
				if ((c == '\n') || (c == '\r'))
					break;
			if (c == '\n')
				++line;
			continue;
		case '.':
			if (!read(c))
				return Dot; //一个三元组结束
			unread();
			if ((c >= '0') && (c <= '9'))
				return lexNumber(token, '.');
			return Dot;
		case ':':
			return Colon;
		case ';':
			return Semicolon;
		case ',':
			return Comma;
		case '[':
			return LBracket;
		case ']':
			return RBracket;
		case '(':
			return LParen;
		case ')':
			return RParen;
		case '@':
			return At;
		case '+':
		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			return lexNumber(token, c);
		case '^':
			if ((!read(c)) || (c != '^')) {
				cerr << "lexer error in line " << line << ": '^' expected" << endl;
				throw Exception();
			}
			return Type;
		case '\"':
			return lexString(token, c);
		case '<':
			return lexURI(token, c);
		default:
			if (((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z')) || (c == '_')) { // XXX unicode!
				token = c;
				while (read(c)) {
					if (issep(c)) {
						unread();
						break;
					}
					token += c;
				}
				if (token == "a")
					return A;
				if (token == "true")
					return True;
				if (token == "false")
					return False;
				return Name;
			} else {
				cerr << "lexer error in line " << line << ": unexpected character " << c << endl;
				throw Exception();
			}
		}
	}
	return Eof;
}
// Constructor
TurtleParser::TurtleParser(istream& in) :lexer(in), triplesReader(0), nextBlank(0){}
// Destructor
TurtleParser::~TurtleParser(){}
// Report an error
void TurtleParser::parseError(const string& message)
{
	cerr << "parse error in line " << lexer.getLine() << ": " << message
			<< endl;
	throw Exception();
}
// Construct a new blank node
void TurtleParser::newBlankNode(std::string& node)
{
	stringstream buffer;
	buffer << "_:_" << (nextBlank++);
	node = buffer.str();
}
// Parse a directive
void TurtleParser::parseDirective()
{
	//cout<<"parse directive"<<endl;
	std::string value;
	if (lexer.next(value) != Lexer::Name)
		parseError("directive name expected after '@'");

	if (value == "base") {
		if (lexer.next(base) != Lexer::URI)
			parseError("URI expected after @base");
		static bool warned = false;
		if (!warned) {
			cerr << "warning: @base directives are currently ignored" << endl;
			warned = true; // XXX
		}
	} else if (value == "prefix") {
		std::string prefixName;
		Lexer::Token token = lexer.next(prefixName);
		// A prefix name?
		if (token == Lexer::Name) {
			token = lexer.next();
		} else
			prefixName.resize(0);
		// Colon
		if (token != Lexer::Colon)
			parseError("':' expected after @prefix");
		// URI
		std::string uri;
		if (lexer.next(uri) != Lexer::URI)
			parseError("URI expected after @prefix");
		prefixes[prefixName] = uri;
	} else {
		parseError("unknown directive @" + value);
	}

	// Final dot
	if (lexer.next() != Lexer::Dot)
		parseError("'.' expected after directive");
}
// Is a (generalized) name token?
inline bool TurtleParser::isName(Lexer::Token token)
{
	return (token == Lexer::Name) || (token == Lexer::A)
			|| (token == Lexer::True) || (token == Lexer::False);
}
// Parse a qualified name
void TurtleParser::parseQualifiedName(const string& prefix, string& name)
{
	//cout<<"parseQualifiedName"<<endl;
	if (lexer.next() != Lexer::Colon)
		parseError("':' expected in qualified name");
	if (!prefixes.count(prefix))
		parseError("unknown prefix '" + prefix + "'");
	string expandedPrefix = prefixes[prefix];
	Lexer::Token token = lexer.next(name);
	if (isName(token)) {
		name = expandedPrefix + name;
	} else {
		lexer.unget(token, name);
		name = expandedPrefix;
	}
}
// Parse a blank entry
void TurtleParser::parseBlank(std::string& entry)
{
	Lexer::Token token = lexer.next(entry);
	switch (token) {
	case Lexer::Name:
		if ((entry != "_") || (lexer.next() != Lexer::Colon) || (!isName(lexer.next(entry))))
			parseError("blank nodes must start with '_:'");
		entry = "_:" + entry;
		return;
	case Lexer::LBracket: {
		newBlankNode(entry);
		token = lexer.next();
		if (token != Lexer::RBracket) {
			lexer.ungetIgnored(token);
			std::string predicate, object;
			Type::ID objectType;
			parsePredicateObjectList(entry, predicate, object, objectType);
			triples.push_back(Triple(entry, predicate, object, objectType));
			if (lexer.next() != Lexer::RBracket)
				parseError("']' expected");
		}
		return;
	}
	case Lexer::LParen: {
		// Collection
		vector<string> entries;
		vector<Type::ID> entryTypes;
		while ((token = lexer.next()) != Lexer::RParen) {
			lexer.ungetIgnored(token);
			entries.push_back(string());
			entryTypes.push_back(Type::URI);
			parseObject(entries.back(), entryTypes.back());
		}

		// Empty collection?
		if (entries.empty()) {
			entry = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
			return;
		}

		// Build blank nodes
		vector<string> nodes;
		nodes.resize(entries.size());
		for (unsigned index = 0; index < entries.size(); index++)
			newBlankNode(nodes[index]);
		nodes.push_back("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");

		// Derive triples
		for (unsigned index = 0; index < entries.size(); index++) {
			triples.push_back(
					Triple(nodes[index],
							"http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
							entries[index], entryTypes[index]));
			triples.push_back(
					Triple(nodes[index],
							"http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
							nodes[index + 1], Type::URI));
		}
		entry = nodes.front();
	}
		return;
	default:
		parseError("invalid blank entry");
	}
}
// Parse a subject通过token的值来给subject值
void TurtleParser::parseSubject(Lexer::Token token, std::string& subject)
{
	switch (token) {
	case Lexer::URI:
		// URI
		return;
	case Lexer::A:
		subject = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		return;
	case Lexer::Colon:
		// Qualified name with empty prefix? 冒号
		lexer.unget(token, subject);
		parseQualifiedName("", subject);
		return;
	case Lexer::Name:
		// Qualified name
		// Blank node?
		if (subject == "_") {
			lexer.unget(token, subject);
			parseBlank(subject);
			return;
		}
		// No
		parseQualifiedName(subject, subject);
		return;
	case Lexer::LBracket:
	case Lexer::LParen:
		// Opening bracket/parenthesis
		lexer.unget(token, subject);
		parseBlank(subject);
	default:
		parseError("invalid subject");
	}
}
// Parse an object
void TurtleParser::parseObject(std::string& object, Type::ID& objectType)
{
	Lexer::Token token = lexer.next(object);
	switch (token) {
	case Lexer::URI:
		// URI
		objectType = Type::URI;
		return;
	case Lexer::Colon:
		// Qualified name with empty prefix?
		lexer.unget(token, object);
		parseQualifiedName("", object);
		objectType = Type::URI;
		return;
	case Lexer::Name:
		// Qualified name
		// Blank node?
		if (object == "_") {
			lexer.unget(token, object);
			parseBlank(object);
			objectType = Type::URI;
			return;
		}
		// No
		parseQualifiedName(object, object);
		objectType = Type::URI;
		return;
	case Lexer::LBracket:
	case Lexer::LParen:
		// Opening bracket/parenthesis
		lexer.unget(token, object);
		parseBlank(object);
		objectType = Type::URI;
		return;
	case Lexer::Integer:
		objectType = Type::Integer;
		return;
	case Lexer::Decimal:
		objectType = Type::Decimal;
		return;
	case Lexer::Double:
		objectType = Type::Double;
		return;
	case Lexer::A:
		objectType = Type::URI;
		return;
	case Lexer::True:
		objectType = Type::Boolean;
		return;
	case Lexer::False:
		// Literal
		objectType = Type::Boolean;
		return;
	case Lexer::String:
		// String literal
		{
			token = lexer.next();
			if (token == Lexer::At) {
				if (lexer.next() != Lexer::Name)
					parseError("language tag expected");
				objectType = Type::CustomLanguage;
			} else if (token == Lexer::Type) {
				string type;
				token = lexer.next(type);
				if (token == Lexer::URI) {
					// Already parsed
				} else if (token == Lexer::Colon) {
					parseQualifiedName("", type);
				} else if (token == Lexer::Name) {
					parseQualifiedName(type, type);
				}
				if (type == "http://www.w3.org/2001/XMLSchema#string") {
					objectType = Type::String;
				} else if (type == "http://www.w3.org/2001/XMLSchema#integer") {
					objectType = Type::Integer;
				} else if (type == "http://www.w3.org/2001/XMLSchema#decimal") {
					objectType = Type::Decimal;
				} else if (type == "http://www.w3.org/2001/XMLSchema#double") {
					objectType = Type::Double;
				} else if (type == "http://www.w3.org/2001/XMLSchema#boolean") {
					objectType = Type::Boolean;
				} else if (type == "http://www.w3.org/2001/XMLSchema#dateTime") {
					objectType = Type::Date;
				} else {
					objectType = Type::String;
				}
				/*
				 static bool warned = false;
				 if (!warned) {
				 cerr << "warning: literal types are currently ignored" << endl;
				 warned = true; // XXX
				 }*/
			} else {
				lexer.ungetIgnored(token);
			}
			return;
		}
	default:
		parseError("invalid object");
	}
}

/*void TurtleParser::parsePredicateObjectList(const string& subject,
		string& predicate, string& object)
		// Parse a predicate object list
		{
	// Parse the first predicate
	Lexer::Token token;
	switch (token = lexer.next(predicate)) {
	case Lexer::URI:
		break;
	case Lexer::A:
		predicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		break;
	case Lexer::Colon:
		lexer.unget(token, predicate);
		parseQualifiedName("", predicate);
		break;
	case Lexer::Name:
		if (predicate == "_")
			parseError("blank nodes not allowed as predicate");
		parseQualifiedName(predicate, predicate);
		break;
	default:
		parseError("invalid predicate");
	}

	// Parse the object
	parseObject(object);
	if (columns != NULL && strcmp(columns, "--4") == 0) {
		string fouthcol; //wo 为了btc改
		parseObject(fouthcol); //wo 为了btc改
	}

	// Additional objects?
	token = lexer.next();
	while (token == Lexer::Comma) { //逗号，多个O放在一起
		string additionalObject;
		parseObject(additionalObject);
		triples.push_back(Triple(subject, predicate, additionalObject));
		token = lexer.next();
	}

	// Additional predicates?
	while (token == Lexer::Semicolon) { //分号，省略S（S相同），
		// Parse the predicate
		string additionalPredicate;
		switch (token = lexer.next(additionalPredicate)) {
		case Lexer::URI:
			break;
		case Lexer::A:
			additionalPredicate =
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
			break;
		case Lexer::Colon:
			lexer.unget(token, additionalPredicate);
			parseQualifiedName("", additionalPredicate);
			break;
		case Lexer::Name:
			if (additionalPredicate == "_")
				parseError("blank nodes not allowed as predicate");
			parseQualifiedName(additionalPredicate, additionalPredicate);
			break;
		default:
			lexer.unget(token, additionalPredicate);
			return;
		}

		// Parse the object
		string additionalObject;
		parseObject(additionalObject);
		triples.push_back(
				Triple(subject, additionalPredicate, additionalObject));

		// Additional objects?
		token = lexer.next();
		while (token == Lexer::Comma) {
			parseObject(additionalObject);
			triples.push_back(
					Triple(subject, additionalPredicate, additionalObject));
			token = lexer.next();
		}
	}
	lexer.ungetIgnored(token);
}*/

// Parse a predicate object list
void TurtleParser::parsePredicateObjectList(const string& subject,string& predicate, string& object, Type::ID& objectType)
{
	//cout<<"now subject is"<<subject<<endl;
	//Parse the first predicate
	Lexer::Token token;
	switch (token = lexer.next(predicate)) {
	case Lexer::URI:
		break;
	case Lexer::A:
		predicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		break;
	case Lexer::Colon:
		lexer.unget(token, predicate);
		parseQualifiedName("", predicate);
		break;
	case Lexer::Name:
		if (predicate == "_")
			parseError("blank nodes not allowed as predicate");
		parseQualifiedName(predicate, predicate);
		break;
	default:
		parseError("invalid predicate");
	}

	// Parse the object
	parseObject(object,objectType);
	if (columns != NULL && strcmp(columns, "--4") == 0) {
		string fouthcol; //wo 为了btc改
		Type::ID fouthobjectType;
		parseObject(fouthcol,fouthobjectType); //wo 为了btc改
	}

	// Additional objects?
	token = lexer.next();
	while (token == Lexer::Comma) { //逗号，多个O放在一起
		string additionalObject;
		Type::ID additionalObjectType;
		parseObject(additionalObject, additionalObjectType);
		triples.push_back(Triple(subject, predicate, additionalObject,additionalObjectType));
		token = lexer.next();
	}

	// Additional predicates?
	while (token == Lexer::Semicolon) { //分号，省略S（S相同），
		// Parse the predicate
		string additionalPredicate;
		switch (token = lexer.next(additionalPredicate)) {
		case Lexer::URI:
			break;
		case Lexer::A:
			additionalPredicate =
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
			break;
		case Lexer::Colon:
			lexer.unget(token, additionalPredicate);
			parseQualifiedName("", additionalPredicate);
			break;
		case Lexer::Name:
			if (additionalPredicate == "_")
				parseError("blank nodes not allowed as predicate");
			parseQualifiedName(additionalPredicate, additionalPredicate);
			break;
		default:
			lexer.unget(token, additionalPredicate);
			return;
		}

		// Parse the object
		string additionalObject;
		Type::ID additionalObjectType;
		parseObject(additionalObject, additionalObjectType);
		triples.push_back(
				Triple(subject, additionalPredicate, additionalObject,
						additionalObjectType));

		// Additional objects?
		token = lexer.next();
		while (token == Lexer::Comma) {
			parseObject(additionalObject, additionalObjectType);
			triples.push_back(
					Triple(subject, additionalPredicate, additionalObject,
							additionalObjectType));
			token = lexer.next();
		}
	}
	lexer.ungetIgnored(token);
}

/*
//---------------------------------------------------------------------------
//void TurtleParser::parseTriple(Lexer::Token token, std::string& subject,
//		std::string& predicate, std::string& object)
//		// Parse a triple
//		{
//	parseSubject(token, subject);
//	parsePredicateObjectList(subject, predicate, object);
//	if (lexer.next() != Lexer::Dot)
//		parseError("'.' expected after triple");
//}
// Parse a triple
*/

void TurtleParser::parseTriple(Lexer::Token token, std::string& subject,std::string& predicate, std::string& object, Type::ID& objectType)
{
	parseSubject(token, subject);
	parsePredicateObjectList(subject, predicate, object, objectType);
	if (lexer.next() != Lexer::Dot)
		parseError("'.' expected after triple");
}
//Read the next triple
bool TurtleParser::parse(std::string& subject, std::string& predicate,std::string& object, Type::ID& objectType) {
	// Some triples left?
	if (triplesReader < triples.size()) { //std::vector<Triple> triples;这里的size是元素个数
		subject = triples[triplesReader].subject;
		predicate = triples[triplesReader].predicate;
		object = triples[triplesReader].object;
		objectType = triples[triplesReader].objectType;
		if ((++triplesReader) >= triples.size()) {
			triples.clear(); //清除元素
			triplesReader = 0;
		}
		return true;
	}

	// No, check if the input is done
	Lexer::Token token;
	while (true) {
		token = lexer.next(subject);
		if (token == Lexer::Eof)
			return false;

		// A directive?
		if (token == Lexer::At) {
			parseDirective();
			continue;
		} else
			break;
	}

	// No, parse a triple
	parseTriple(token, subject, predicate, object, objectType);
	return true;

}

/*
//---------------------------------------------------------------------------
bool TurtleParser::parse(std::string& subject, std::string& predicate,
		std::string& object)
		// Read the next triple
		{
	// Some triples left?
	if (triplesReader < triples.size()) { //std::vector<Triple> triples;这里的size是元素个数
		subject = triples[triplesReader].subject;
		predicate = triples[triplesReader].predicate;
		object = triples[triplesReader].object;
		if ((++triplesReader) >= triples.size()) {
			triples.clear(); //清除元素
			triplesReader = 0;
		}
		return true;
	}

	// No, check if the input is done
	Lexer::Token token;
	while (true) {
		token = lexer.next(subject);
		if (token == Lexer::Eof)
			return false;

		// A directive?
		if (token == Lexer::At) {
			parseDirective();
			continue;
		} else
			break;
	}

	// No, parse a triple
	parseTriple(token, subject, predicate, object);
	return true;
}
*/
