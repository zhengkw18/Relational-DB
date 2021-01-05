/* Modified from https://raw.githubusercontent.com/thinkpad20/sql/master/src/yacc/sql.y
             and https://github.com/miskcoo/TrivialDB
 * Grammar: http://h2database.com/html/grammar.html#select */

%define parse.error verbose

%{
#include <stdio.h>
#include <stdlib.h>
#include "defs.h"
#include "execute.h"

void yyerror(const char *s);

#include "sql.yy.c"

%}

%union {
	char *val_s;
	int   val_i;
	float val_f;
	struct field_item_t       *field_items;
	struct table_def_t        *table_def;
	struct column_ref_t       *column_ref;
	struct linked_list_t      *list;
	struct table_constraint_t *constraint;
	struct insert_info_t      *insert_info;
	struct update_info_t      *update_info;
	struct delete_info_t      *delete_info;
	struct select_info_t      *select_info;
	struct expr_node_t        *expr;
}

%token TRUE FALSE NULL_TOKEN MIN MAX SUM AVG COUNT
%token LIKE IS OR AND NOT NEQ GEQ LEQ
%token INTEGER FLOAT CHAR VARCHAR DATE
%token INTO FROM WHERE VALUES JOIN INNER OUTER
%token LEFT RIGHT FULL ASC DESC ORDER BY IN ON AS TO
%token DISTINCT GROUP USING INDEX TABLE DATABASE DATABASES
%token DEFAULT UNIQUE PRIMARY FOREIGN REFERENCES CHECK KEY OUTPUT RENAME CONSTRAINT
%token USE CREATE DROP SELECT INSERT UPDATE DELETE SHOW SET EXIT ALTER ADD COPY

%token IDENTIFIER
%token DATE_LITERAL
%token STRING_LITERAL
%token FLOAT_LITERAL
%token INT_LITERAL

%type <val_s> IDENTIFIER STRING_LITERAL DATE_LITERAL
%type <val_f> FLOAT_LITERAL
%type <val_i> INT_LITERAL

%type <val_i> field_type field_width field_flag field_flags
%type <val_s> name

%type <field_items> table_field
%type <table_def> create_table_stmt
%type <column_ref> column_ref
%type <constraint> table_extra_option
%type <list> column_list expr_list insert_values literal_list table_fields
%type <list> table_extra_options table_extra_option_list
%type <insert_info> insert_stmt insert_columns
%type <update_info> update_stmt
%type <delete_info> delete_stmt
%type <select_info> select_stmt
%type <expr> expr factor term condition cond_term where_clause literal literal_list_expr
%type <expr> aggregate_expr aggregate_term select_expr default_expr
%type <val_i> logical_op compare_op aggregate_op
%type <list> select_expr_list select_expr_list_s table_refs

%start sql_stmts

%%

sql_stmts  :  sql_stmt
		   |  sql_stmts sql_stmt
		   ;

sql_stmt   :  create_table_stmt ';' 	{ execute_create_table($1); }
		   |  CREATE DATABASE name ';' 	{ execute_create_database($3); }
		   |  USE name ';'    	       	{ execute_use_database($2); }
		   |  SHOW DATABASE name ';'   	{ execute_show_database($3); }
		   |  SHOW DATABASES ';'       	{ execute_show_databases(); }
		   |  DROP DATABASE name ';'   	{ execute_drop_database($3); }
		   |  SHOW TABLE name ';'      	{ execute_show_table($3); }
		   |  DROP TABLE name ';'      	{ execute_drop_table($3); }
		   |  insert_stmt ';'          	{ execute_insert($1); }
		   |  update_stmt ';'          	{ execute_update($1); }
		   |  delete_stmt ';'          	{ execute_delete($1); }
		   |  select_stmt ';'          	{ execute_select($1); }
		   |  EXIT ';'                 	{ execute_quit(); YYACCEPT; }
		   |  CREATE INDEX name ON name '(' column_list ')' ';' 			{ execute_create_index($5, $3, $7); }
		   |  DROP   INDEX name ON name ';' 								{ execute_drop_index($5, $3); }
		   |  ALTER TABLE name ADD INDEX name '(' column_list ')' ';' 		{ execute_create_index($3, $6, $8); }
		   |  ALTER TABLE name DROP INDEX name ';'							{ execute_drop_index($3, $6); }
		   |  ALTER TABLE name ADD PRIMARY KEY '(' column_list ')' ';' 		{ execute_add_primary($3, $8); }
		   |  ALTER TABLE name DROP PRIMARY KEY ';' 						{ execute_drop_primary($3); }
		   |  ALTER TABLE name ADD UNIQUE '(' name ')' ';' 					{ execute_add_unique($3, $7); }
		   |  ALTER TABLE name DROP UNIQUE '(' name ')' ';' 				{ execute_drop_unique($3, $7); }
		   |  ALTER TABLE name RENAME TO name ';' 							{ execute_rename($3, $6); }
		   |  ALTER TABLE name ADD CONSTRAINT name FOREIGN KEY '(' column_list ')' REFERENCES name '(' column_list ')' ';'  
			   																{ execute_add_foreign($3, $6, $10, $13, $15); }
		   |  ALTER TABLE name DROP FOREIGN KEY name ';' 					{ execute_drop_foreign($3, $7); }
		   |  ALTER TABLE name ADD table_field ';' 							{ execute_add_col($3, $5); }
		   |  ALTER TABLE name DROP name ';' 								{ execute_drop_col($3, $5); }
		   |  COPY name FROM STRING_LITERAL ';'								{ execute_copy_from($2, $4); }
		   ;

create_table_stmt : CREATE TABLE name '(' table_fields table_extra_options ')' {
				  	$$ = (table_def_t*)malloc(sizeof(table_def_t));
					$$->name = $3;
					$$->fields = $5;
					$$->constraints = $6;
				  }
				  ;
insert_stmt          : INSERT INTO insert_columns VALUES insert_values {
					 	$$ = $3;
						$$->values = $5;
					 }
					 ;

insert_values        : '(' expr_list ')' {
					 	$$ = $2;
					 }
					 ;

insert_columns       : name {
					 	$$ = (insert_info_t*)malloc(sizeof(insert_info_t));
						$$->table   = $1;
						$$->columns = NULL;
						$$->values  = NULL;
					 }
					 | name '(' column_list ')' {
					 	$$ = (insert_info_t*)malloc(sizeof(insert_info_t));
						$$->table   = $1;
						$$->columns = $3;
						$$->values  = NULL;
					 }
					 ;

delete_stmt         : DELETE FROM name where_clause {
					 	$$ = (delete_info_t*)malloc(sizeof(delete_info_t));
						$$->table = $3;
						$$->where = $4;
					}
					;

update_stmt         : UPDATE name SET name '=' expr where_clause {
					 	$$ = (update_info_t*)malloc(sizeof(update_info_t));
						$$->table = $2;
						$$->value = $6;
						$$->where = $7;
						$$->column = $4;
					}
					;

select_stmt         : SELECT select_expr_list_s FROM table_refs where_clause {
					 	$$ = (select_info_t*)malloc(sizeof(select_info_t));
						$$->tables = $4;
						$$->exprs  = $2;
						$$->where  = $5;
					}
					;

table_refs          : table_refs ',' name {
						$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
						$$->data = $3;
						$$->next = $1;
					}
					| name {
						$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
						$$->data = $1;
						$$->next = NULL;
					}
					;

select_expr_list_s  : select_expr_list { $$ = $1; }
					| '*'              { $$ = NULL; }

select_expr_list    : select_expr_list ',' select_expr {
						$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
						$$->data = $3;
						$$->next = $1;
					}
					| select_expr {
						$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
						$$->data = $1;
						$$->next = NULL;
					}
					;

select_expr         : expr            { $$ = $1; }
					| aggregate_expr  { $$ = $1; }

aggregate_expr      : aggregate_op '(' aggregate_term ')' {
						$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
						$$->left  = $3;
						$$->op    = $1;
					}
					| COUNT '(' aggregate_term ')' {
						$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
						$$->left  = $3;
						$$->op    = OPERATOR_COUNT;
					}
					| COUNT '(' '*' ')' {
						$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
						$$->left  = NULL;
						$$->op    = OPERATOR_COUNT;
					}
					;

aggregate_term      : column_ref {
						$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
						$$->column_ref = $1;
						$$->term_type  = TERM_COLUMN_REF;
					}
					;

aggregate_op        : SUM   { $$ = OPERATOR_SUM; }
					| AVG   { $$ = OPERATOR_AVG; }
					| MIN   { $$ = OPERATOR_MIN; }
					| MAX   { $$ = OPERATOR_MAX; }
					;

where_clause        : WHERE condition { $$ = $2; }
					| /* empty */     { $$ = NULL; }
					;

table_extra_options : ',' table_extra_option_list  { $$ = $2; }
					| /* empty */                  { $$ = NULL; }
					;

table_extra_option_list : table_extra_option_list ',' table_extra_option {
							$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
							$$->data = $3;
							$$->next = $1;
						}
						| table_extra_option {
							$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
							$$->data = $1;
							$$->next = NULL;
						}
						;

table_extra_option : PRIMARY KEY '(' column_list ')' {
				   	$$ = (table_constraint_t*)calloc(1, sizeof(table_constraint_t));
					$$->columns = $4;
					$$->type = TABLE_CONSTRAINT_PRIMARY_KEY;
				   }
				   | CONSTRAINT name FOREIGN KEY '(' column_list ')' REFERENCES name '(' column_list ')' {
				   	$$ = (table_constraint_t*)calloc(1, sizeof(table_constraint_t));
					$$->foreign_name = $2;
					$$->columns = $6;
					$$->foreign_table_ref = $9;
					$$->foreign_column_ref = $11;
					$$->type = TABLE_CONSTRAINT_FOREIGN_KEY;
				   }
				   | CHECK '(' condition ')' {
				   	$$ = (table_constraint_t*)calloc(1, sizeof(table_constraint_t));
					$$->type = TABLE_CONSTRAINT_CHECK;
					$$->check_cond = $3;
				   }
				   | INDEX name '(' column_list ')' {
					$$ = (table_constraint_t*)calloc(1, sizeof(table_constraint_t));
					$$->index_name = $2;
					$$->columns = $4;
					$$->type = TABLE_CONSTRAINT_INDEX;
				   }
				   ;

column_ref   : name {
			 	$$ = (column_ref_t*)malloc(sizeof(column_ref_t));
				$$->table  = NULL;
				$$->column = $1;
			 }
			 | name '.' name {
			 	$$ = (column_ref_t*)malloc(sizeof(column_ref_t));
				$$->table  = $1;
				$$->column = $3;
			 }
			 ;

column_list  : column_list ',' name {
				$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $3;
				$$->next = $1;
			 }
			 | name {
			 	$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $1;
				$$->next = NULL;
			 }
			 ;


table_fields : table_fields ',' table_field {
	 			$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $3;
				$$->next = $1;
	 		 }
			 | table_field {
				$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $1;
				$$->next = NULL;
			 }
			 ;

table_field  : name field_type field_width field_flags default_expr {
			 	$$ = (field_item_t*)malloc(sizeof(field_item_t));
				$$->name = $1;
				$$->type = $2;
				$$->width = $3;
				$$->flags = $4;
				$$->default_value = $5;
			 }
			 ;

default_expr : DEFAULT literal { $$ = $2; }
			 | /* empty */     { $$ = NULL; }

field_flags : field_flags field_flag  { $$ = $1 | $2; }
			| /* empty */             { $$ = 0; }
			;

field_flag  : NOT NULL_TOKEN  { $$ = FIELD_FLAG_NOTNULL; }
			| UNIQUE          { $$ = FIELD_FLAG_UNIQUE; }
			;

field_width : '(' INT_LITERAL ')'  { $$ = $2; }
			| /* empty */          { $$ = 0; }
			;

field_type  : INTEGER { $$ = FIELD_TYPE_INT; }
		    | FLOAT   { $$ = FIELD_TYPE_FLOAT; }
		    | CHAR    { $$ = FIELD_TYPE_CHAR; }
		    | DATE    { $$ = FIELD_TYPE_DATE; }
		    | VARCHAR { $$ = FIELD_TYPE_VARCHAR; }
		    ;

logical_op : AND  { $$ = OPERATOR_AND; }
		   | OR   { $$ = OPERATOR_OR; }
		   ;

compare_op : '='  { $$ = OPERATOR_EQ; }
		   | '<'  { $$ = OPERATOR_LT; }
		   | '>'  { $$ = OPERATOR_GT; }
		   | LEQ  { $$ = OPERATOR_LEQ; }
		   | GEQ  { $$ = OPERATOR_GEQ; }
		   | NEQ  { $$ = OPERATOR_NEQ; }
		   | LIKE { $$ = OPERATOR_LIKE; }
		   ;

condition  : condition logical_op cond_term {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $3;
				$$->op    = $2;
		   }
		   | cond_term { $$ = $1; }
		   ;

cond_term  : expr compare_op expr {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $3;
				$$->op    = $2;
		   }
		   | expr IN '(' literal_list_expr ')' {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $4;
				$$->op    = OPERATOR_IN;
		   }
		   | expr IS NULL_TOKEN {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->op    = OPERATOR_ISNULL;
		   }
		   | expr IS NOT NULL_TOKEN {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->op    = OPERATOR_NOTNULL;
		   }
		   | NOT cond_term {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $2;
				$$->op    = OPERATOR_NOT;
		   }
		   | '(' condition ')' { $$ = $2; }
		   | TRUE {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->val_b     = 1;
				$$->term_type = TERM_BOOL;
		   }
		   | FALSE {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->val_b     = 0;
				$$->term_type = TERM_BOOL;
		   }
		   ;

expr_list  : expr_list ',' expr {
				$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $3;
				$$->next = $1;
		   }
		   | expr {
				$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $1;
				$$->next = NULL;
		   }
		   ;

expr       : expr '+' factor {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $3;
				$$->op    = OPERATOR_ADD;
		   }
		   | expr '-' factor {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $3;
				$$->op    = OPERATOR_MINUS;
		   }
		   | factor { $$ = $1; }
		   ;

factor     : factor '*' term {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $3;
				$$->op    = OPERATOR_MUL;
		   }
		   | factor '/' term {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $1;
				$$->right = $3;
				$$->op    = OPERATOR_DIV;
		   }
		   | term { $$ = $1; }
		   ;

term       : column_ref {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->column_ref = $1;
				$$->term_type  = TERM_COLUMN_REF;
		   }
		   | '-' term {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->left  = $2;
				$$->op    = OPERATOR_NEGATE;
		   }
		   | literal      { $$ = $1; }
		   | NULL_TOKEN {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->term_type  = TERM_NULL;
		   }
		   | '(' expr ')' { $$ = $2; }
		   ;

literal    : INT_LITERAL {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->val_i      = $1;
				$$->term_type  = TERM_INT;
		   }
		   | FLOAT_LITERAL {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->val_f      = $1;
				$$->term_type  = TERM_FLOAT;
		   }
		   | DATE_LITERAL {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->val_s      = $1;
				$$->term_type  = TERM_DATE;
		   }
		   | STRING_LITERAL {
		   		$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
				$$->val_s      = $1;
				$$->term_type  = TERM_STRING;
		   }
		   ;

literal_list : literal_list ',' literal {
				$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $3;
				$$->next = $1;
			 }
			 | literal {
				$$ = (linked_list_t*)malloc(sizeof(linked_list_t));
				$$->data = $1;
				$$->next = NULL;
			 }
			 ;

literal_list_expr : literal_list {
					$$ = (expr_node_t*)calloc(1, sizeof(expr_node_t));
					$$->literal_list = $1;
					$$->term_type    = TERM_LITERAL_LIST;
				  }
				  ;

name : IDENTIFIER          { $$ = $1; }
		   | '`' IDENTIFIER '`'  { $$ = $2; }
		   ;

%%

void yyerror(const char *msg)
{
	fprintf(stderr, "[Error] %s\n", msg);
}

int yywrap()
{
	return 1;
}

char run_parser(const char *input)
{
	char ret;
	if(input) {
		YY_BUFFER_STATE buf = yy_scan_string(input);
		yy_switch_to_buffer(buf);
		ret = yyparse();
		yy_delete_buffer(buf);
	} else {
		ret = yyparse();
	}

	return ret;
}
char run_parser_from_file(const char *filename)
{
	char ret;
	FILE* file = fopen(filename, "r");
	if(!file) {
		fprintf(stderr, "[Error] File not found.\n");
        return 0;
	}
	yyrestart(file);
  	ret = yyparse();
  	fclose(yyin);
	yyrestart(stdin);
	return ret;
}