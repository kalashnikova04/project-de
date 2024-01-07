create table klsn.klsn_dwh_fact_transactions (
	trans_id varchar(200),
	trans_date timestamp(0),
 	card_num varchar(20),
 	oper_type varchar(200),
 	amt decimal,
 	oper_result varchar(200),
 	terminal varchar(200)
);

create table klsn.klsn_stg_transactions (
	trans_id varchar(200),
	trans_date timestamp(0),
 	card_num varchar(20),
 	oper_type varchar(200),
 	amt decimal,
 	oper_result varchar(200),
 	terminal varchar(200)
);


create table klsn.klsn_dwh_fact_passport_blacklist (
	passport_num varchar(200),
	entry_dt date
);

create table klsn.klsn_stg_blacklist (
	passport_num varchar(200),
	entry_dt date
);


create table klsn.klsn_dwh_dim_terminals (
	terminal_id varchar(200),
	terminal_type varchar(200),
 	terminal_city varchar(200),
 	terminal_address varchar(200),
 	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table klsn.klsn_stg_terminals (
	terminal_id varchar(200),
	terminal_type varchar(200),
 	terminal_city varchar(200),
 	terminal_address varchar(200),
	update_dt timestamp(0)
);


create table klsn.klsn_dwh_dim_cards (
	card_num varchar(20),
	account_num varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table klsn.klsn_stg_cards (
	card_num varchar(20),
	account_num varchar(20),
	update_dt timestamp(0)
);


create table klsn.klsn_dwh_dim_accounts (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table klsn.klsn_stg_accounts (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	update_dt timestamp(0)
);


create table klsn.klsn_dwh_dim_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(18),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table klsn.klsn_stg_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(18),
	update_dt timestamp(0)
);


create table klsn.klsn_rep_fraud (
	event_dt date,
	passport varchar(15),
	fio varchar(200),
	phone varchar(18),
	event_type varchar(200),
	report_dt date
);

create table klsn.klsn_meta_proj (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

create table klsn.klsn_stg_terminals_del( 
	terminal_id varchar(10)
);

create table klsn.klsn_stg_cards_del( 
	card_num varchar(20)
);

create table klsn.klsn_stg_accounts_del( 
	account_num varchar(20)
);

create table klsn.klsn_stg_clients_del( 
	client_id varchar(10)
);