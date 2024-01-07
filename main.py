#!/usr/bin/env python
import psycopg2
import pandas as pd
import os
import numpy as np

conn = psycopg2.connect(database="db",
                        host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
                        user="hse",
                        password="hsepassword",
                        port="6432")


conn.autocommit = False

cursor = conn.cursor()

# очистка стейджинга
cursor.execute(
    "delete from klsn.klsn_stg_transactions;\
        delete from klsn.klsn_stg_blacklist;\
            delete from klsn.klsn_stg_terminals;\
                delete from klsn.klsn_stg_cards;\
                    delete from klsn.klsn_stg_accounts;\
                        delete from klsn.klsn_stg_clients;")


cursor.execute(
 """ truncate table klsn.klsn_dwh_dim_cards;\
                    truncate table klsn.klsn_dwh_dim_accounts;\
                    truncate table klsn.klsn_dwh_dim_clients;\
                    truncate table klsn.klsn_dwh_dim_terminals;\
                    truncate table klsn.klsn_meta_proj;\
                    truncate table klsn.klsn_dwh_fact_transactions;\
                    truncate table klsn.klsn_dwh_fact_passport_blacklist;""")
# truncate table dwh.XXXX_stg;
# truncate table dwh.XXXX_target;

cur_path = os.getcwd()
files = [f for f in os.listdir('.') if os.path.isfile(f)]
for file in files:
    if file.startswith('transactions'):
        df = pd.read_csv(file, sep=';', header = 0)
        df.transaction_id = df.transaction_id.astype('str')
        df.amount = pd.to_numeric(df.amount.apply(lambda x: x.replace(',', '.')), downcast='float').apply(lambda x: round(x, 2))

cursor.executemany( """ INSERT INTO klsn.klsn_stg_transactions(
                            trans_id,
                            trans_date,
                            amt,
                            card_num,
                            oper_type,
                            oper_result,
                            terminal
                        ) VALUES( %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() )

cursor.execute( """ INSERT INTO klsn.klsn_dwh_fact_transactions
                    SELECT
                        trans_id,
                        trans_date,
                        card_num,
                        oper_type,
                        amt,
                        oper_result,
                        terminal
                    FROM klsn.klsn_stg_transactions""")


for file in files:
    if file.startswith('passport_blacklist'):
        df = pd.read_excel(file, sheet_name='blacklist', header=0, index_col=None)

cursor.executemany( """ INSERT INTO klsn.klsn_stg_blacklist(
                            entry_dt,
                            passport_num
                        ) VALUES( %s, %s )""", df.values.tolist() )

cursor.execute( """ INSERT INTO klsn.klsn_dwh_fact_passport_blacklist
                    SELECT
                        passport_num,
                        entry_dt
                    FROM klsn.klsn_stg_blacklist""")

for file in files:
    if file.startswith('terminals'):
        df = pd.read_excel(file, sheet_name='terminals', header=0, index_col=None)

cursor.executemany( """ INSERT INTO klsn.klsn_stg_terminals(
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address
                        ) VALUES( %s, %s, %s, %s )""", df.values.tolist() )


# загрузка
cursor.execute( """ INSERT INTO klsn.klsn_dwh_dim_terminals( 
                        terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_address,
                        create_dt,
                        update_dt
                    )
                    SELECT
                        stg.terminal_id,
                        stg.terminal_type,
                        stg.terminal_city,
                        stg.terminal_address,
                        stg.update_dt,
                        null
                    FROM klsn.klsn_stg_terminals stg
                    LEFT JOIN klsn.klsn_dwh_dim_terminals tgt
                    ON stg.terminal_id = tgt.terminal_id
                    WHERE tgt.terminal_id IS NULL""" )

# удаление
cursor.execute( """ INSERT INTO klsn.klsn_stg_terminals_del( terminal_id )
                    SELECT terminal_id from klsn.klsn_dwh_dim_terminals """)

cursor.execute( """ DELETE FROM klsn.klsn_dwh_dim_terminals
                    WHERE terminal_id in (
                        SELECT tgt.terminal_id
                        FROM klsn.klsn_dwh_dim_terminals tgt
                        LEFT JOIN klsn.klsn_stg_terminals_del stg
                        ON stg.terminal_id = tgt.terminal_id
                        WHERE stg.terminal_id IS NULL
                    ) """)

cursor.execute( """ UPDATE klsn.klsn_meta_proj
                    SET max_update_dt = coalesce( 
                        (SELECT
                            max(update_dt) 
                        FROM klsn.klsn_stg_terminals),
                        ( SELECT
                            max_update_dt
                        FROM klsn.klsn_meta_proj
                        WHERE schema_name='klsn' and table_name='klsn_dwh_dim_terminals' )
                    )
                    WHERE schema_name='klsn' and table_name='klsn_dwh_dim_terminals' """)


cursor.execute( """ SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        create_dt,
                        update_dt
                    FROM info.clients""" )
records = cursor.fetchall()
names = [ x[0] for x in cursor.description ]
df = pd.DataFrame( records, columns = names )


cursor.executemany( """ INSERT INTO klsn.klsn_stg_clients(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            update_dt
                        ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s )""", np.array(df.values.tolist())[:,:-1] )


# загрузка
cursor.execute( """ INSERT INTO klsn.klsn_dwh_dim_clients( 
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        create_dt,
                        update_dt
                    )
                    SELECT
                        stg.client_id,
                        stg.last_name,
                        stg.first_name,
                        stg.patronymic,
                        stg.date_of_birth,
                        stg.passport_num,
                        stg.passport_valid_to,
                        stg.phone,
                        stg.update_dt,
                        null
                    FROM klsn.klsn_stg_clients stg
                    LEFT JOIN klsn.klsn_dwh_dim_clients tgt
                    ON stg.client_id = tgt.client_id
                    WHERE tgt.client_id IS NULL""" )

# # удаление
# cursor.execute( """ INSERT INTO klsn.klsn_stg_clients_del( client_id )
#                     SELECT client_id from klsn.klsn_dwh_dim_clients """)

# cursor.execute( """ DELETE FROM klsn.klsn_dwh_dim_clients
#                     WHERE client_id in (
#                         SELECT tgt.client_id
#                         FROM klsn.klsn_dwh_dim_clients tgt
#                         LEFT JOIN klsn.klsn_stg_clients_del stg
#                         ON stg.client_id = tgt.client_id
#                         WHERE stg.client_id IS NULL
#                     ) """)

# cursor.execute( """ UPDATE klsn.klsn_meta_proj
#                     SET max_update_dt = coalesce( 
#                         (SELECT
#                             max(update_dt) 
#                         FROM klsn.klsn_stg_accounts),
#                         ( SELECT
#                             max_update_dt
#                         FROM klsn.klsn_meta_proj
#                         WHERE schema_name='klsn' and table_name='klsn_dwh_dim_clients' )
#                     )
#                     WHERE schema_name='klsn' and table_name='klsn_dwh_dim_clients' """)

cursor.execute( """ SELECT
                        account,
                        valid_to,
                        client,
                        create_dt,
                        update_dt
                    FROM info.accounts""" )
records = cursor.fetchall()
names = [ x[0] for x in cursor.description ]
df = pd.DataFrame( records, columns = names )

cursor.executemany( """ INSERT INTO klsn.klsn_stg_accounts(
                            account_num,
                            valid_to,
                            client,
                            update_dt
                        ) VALUES( %s, %s, %s, %s )""", np.array(df.values.tolist())[:,:-1] )

# загрузка
cursor.execute( """ INSERT INTO klsn.klsn_dwh_dim_accounts( 
                        account_num,
                        valid_to,
                        client,
                        create_dt,
                        update_dt
                    )
                    SELECT
                        stg.account_num,
                        stg.valid_to,
                        stg.client,
                        stg.update_dt,
                        null
                    FROM klsn.klsn_stg_accounts stg
                    LEFT JOIN klsn.klsn_dwh_dim_accounts tgt
                    ON stg.account_num = tgt.account_num
                    WHERE tgt.account_num IS NULL""" )

# # удаление
# cursor.execute( """ INSERT INTO klsn.klsn_stg_accounts_del( account_num )
#                     SELECT account_num from klsn.klsn_dwh_dim_accounts """)

# cursor.execute( """ DELETE FROM klsn.klsn_dwh_dim_accounts
#                     WHERE account_num in (
#                         SELECT tgt.account_num
#                         FROM klsn.klsn_dwh_dim_accounts tgt
#                         LEFT JOIN klsn.klsn_stg_accounts_del stg
#                         ON stg.account_num = tgt.account_num
#                         WHERE stg.account_num IS NULL
#                     ) """)

# cursor.execute( """ UPDATE klsn.klsn_meta_proj
#                     SET max_update_dt = coalesce( 
#                         (SELECT
#                             max(update_dt) 
#                         FROM klsn.klsn_stg_accounts),
#                         ( SELECT
#                             max_update_dt
#                         FROM klsn.klsn_meta_proj
#                         WHERE schema_name='klsn' and table_name='klsn_dwh_dim_accounts' )
#                     )
#                     WHERE schema_name='klsn' and table_name='klsn_dwh_dim_accounts' """)

cursor.execute( """ SELECT
                        card_num,
                        account,
                        create_dt,
                        update_dt
                    FROM info.cards""" )
records = cursor.fetchall()
names = [ x[0] for x in cursor.description ]
df = pd.DataFrame( records, columns = names )
df.card_num = df.card_num.apply(lambda x: x[:-1])

cursor.executemany( """ INSERT INTO klsn.klsn_stg_cards( 
                           card_num,
                           account_num,
                           update_dt
                        ) VALUES( %s, %s, %s )""", np.array(df.values.tolist())[:,:-1] )

# загрузка
cursor.execute( """ INSERT INTO klsn.klsn_dwh_dim_cards( 
                        card_num,
                        account_num,
                        create_dt,
                        update_dt
                    )
                    SELECT
                        stg.card_num,
                        stg.account_num,
                        coalesce(stg.update_dt, now()),
                        null
                    FROM klsn.klsn_stg_cards stg
                    LEFT JOIN klsn.klsn_dwh_dim_cards tgt
                    ON stg.card_num = tgt.card_num
                    WHERE tgt.card_num IS NULL""" )

# # удаление
# cursor.execute( """ INSERT INTO klsn.klsn_stg_cards_del( card_num )
#                     SELECT card_num from klsn.klsn_dwh_dim_cards """)

# cursor.execute( """ DELETE FROM klsn.klsn_dwh_dim_cards
#                     WHERE card_num in (
#                         SELECT tgt.card_num
#                         FROM klsn.klsn_dwh_dim_cards tgt
#                         LEFT JOIN klsn.klsn_stg_cards_del stg
#                         ON stg.card_num = tgt.card_num
#                         WHERE stg.card_num IS NULL
#                     ) """)

# cursor.execute( """ UPDATE klsn.klsn_meta_proj
#                     SET max_update_dt = coalesce( 
#                         (SELECT
#                             max(update_dt) 
#                         FROM klsn.klsn_stg_cards),
#                         ( SELECT
#                             max_update_dt
#                         FROM klsn.klsn_meta_proj
#                         WHERE schema_name='klsn' and table_name='klsn_dwh_dim_cards' )
#                     )
#                     WHERE schema_name='klsn' and table_name='klsn_dwh_dim_cards' """)

# cursor.execute(""" INSERT INTO klsn.klsn_meta_proj(
#                         schema_name,
#                         table_name,
#                         max_update_dt
#                     ) VALUES ('klsn', 'klsn_dwh_dim_cards', to_timestamp('1900-01-01','YYYY-MM-DD')),
#                       ('klsn', 'klsn_dwh_dim_accounts', to_timestamp('1900-01-01','YYYY-MM-DD')),
#                       ('klsn', 'klsn_dwh_dim_clients', to_timestamp('1900-01-01','YYYY-MM-DD')),
#                       ('klsn', 'klsn_dwh_dim_terminals', to_timestamp('1900-01-01','YYYY-MM-DD')) """)



# первый тип махинации

cursor.execute( """ insert into klsn.klsn_rep_fraud
                    select 
                        min(kdft.trans_date) as event_dt,
                        kddc2.passport_num as passport_num,
                        (kddc2.last_name||' '||kddc2.first_name||' '||kddc2.patronymic) as fio,
                        kddc2.phone as phone,
                        '1' as event_type,
                        now() as report_dt
                    from klsn.klsn_dwh_fact_transactions kdft
                    inner join klsn.klsn_dwh_dim_cards kddc
                    on kdft.card_num = kddc.card_num
                    inner join klsn.klsn_dwh_dim_accounts kdda
                    on kddc.account_num = kdda.account_num
                    inner join klsn.klsn_dwh_dim_clients kddc2
                    on kddc2.client_id = kdda.client
                    where kdft.oper_result = 'SUCCESS'
                    and 
                        (coalesce(kddc2.passport_valid_to, to_timestamp('2900-01-01','YYYY-MM-DD')) < kdft.trans_date
                        or kddc2.passport_num in (
                            select
                                passport_num
                            from klsn.klsn_dwh_fact_passport_blacklist kdfpb))
                    group by kddc2.passport_num, kddc2.last_name||' '||kddc2.first_name||' '||kddc2.patronymic, kddc2.phone """ )

# второй тип махинации

cursor.execute( """ insert into klsn.klsn_rep_fraud
                    select 
                        min(kdft.trans_date) as event_dt,
                        kddc2.passport_num as passport_num,
                        (kddc2.last_name||' '||kddc2.first_name||' '||kddc2.patronymic) as fio,
                        kddc2.phone as phone,
                        '2' as event_type,
                        now() as report_dt

                    from klsn.klsn_dwh_fact_transactions kdft

                    inner join klsn.klsn_dwh_dim_cards kddc
                    on kdft.card_num = kddc.card_num

                    inner join klsn.klsn_dwh_dim_accounts kdda
                    on kddc.account_num = kdda.account_num

                    inner join klsn.klsn_dwh_dim_clients kddc2
                    on kddc2.client_id = kdda.client

                    where kdda.valid_to < kdft.trans_date
                        and kdft.oper_result = 'SUCCESS' 
                    group by kddc2.passport_num, kddc2.last_name||' '||kddc2.first_name||' '||kddc2.patronymic, kddc2.phone""")

# третий тип махинации

cursor.execute( """ insert into klsn.klsn_rep_fraud
                    select
                        third_type.trans_date as event_dt,
                        kddcl.passport_num as passport_num,
                        kddcl.last_name||' '||kddcl.first_name||' '||kddcl.patronymic as fio,
                        kddcl.phone as phone,
                        '3' as event_type,
                        now() as report_dt
                    from (
                        with lags as (
                            select
                                kdft.trans_date,
                                kdft.card_num,
                                kddt.terminal_city,
                                lag(kddt.terminal_city) over (partition by kdft.card_num order by trans_date) as lag_city,
                                lag(kdft.trans_date) over (partition by kdft.card_num order by trans_date) as lag_time
                            from klsn.klsn_dwh_fact_transactions kdft
                            inner join klsn.klsn_dwh_dim_terminals kddt 
                            on kdft.terminal = kddt.terminal_id
                            where kdft.oper_result = 'SUCCESS'
                        )
                        select
                            min(trans_date) as trans_date,
                            card_num
                        from lags
                        where lag_city != terminal_city and extract( hours from trans_date - lag_time ) = 0
                        group by card_num
                    ) as third_type
                    inner join klsn.klsn_dwh_dim_cards kddc
                    on third_type.card_num = kddc.card_num
                    inner join klsn.klsn_dwh_dim_accounts kdda 
                    on kddc.account_num = kdda.account_num
                    inner join klsn.klsn_dwh_dim_clients kddcl 
                    on kdda.client = kddcl.client_id  """ )

# четвертый тип махинации

cursor.execute( """ insert into klsn.klsn_rep_fraud
                    select
                        fourth_type.trans_date as event_dt,
                        kddcl.passport_num as passport,
                        kddcl.last_name||' '||kddcl.first_name||' '||kddcl.patronymic as fio,
                        kddcl.phone,
                        '4' as event_type,
                        now() as report_dt
                    from (
                        with lags as (
                            select
                                trans_date,
                                lag(trans_date, 3) over (partition by card_num order by trans_date) as lag_time,
                                card_num,
                                amt,
                                lag(amt) over (partition by card_num order by trans_date) as lag_amt1,
                                lag(amt, 2) over (partition by card_num order by trans_date) as lag_amt2,
                                lag(amt, 3) over (partition by card_num order by trans_date) as lag_amt3,
                                oper_result,
                                lag(oper_result) over (partition by card_num order by trans_date) as lag_oper1,
                                lag(oper_result, 2) over (partition by card_num order by trans_date) as lag_oper2,
                                lag(oper_result, 3) over (partition by card_num order by trans_date) as lag_oper3 
                            from klsn.klsn_dwh_fact_transactions
                        )
                        select
                            *
                        from lags
                        where lag_amt3 - lag_amt2 > 0
                            and lag_amt2 - lag_amt1 > 0
                            and lag_amt1 - amt > 0
                            and extract( epoch from (trans_date - lag_time) ) / 60 <= 20
                            and oper_result = 'REJECT'
                            and lag_oper1 = 'REJECT'
                            and lag_oper2 = 'REJECT'
                            and lag_oper3 = 'SUCCESS'
                    ) as fourth_type
                    inner join klsn.klsn_dwh_dim_cards kddc
                    on fourth_type.card_num = kddc.card_num 
                    inner join klsn.klsn_dwh_dim_accounts kdda 
                    on kddc.account_num = kdda.account_num 
                    inner join klsn.klsn_dwh_dim_clients kddcl
                    on kdda.client = kddcl.client_id """ )
conn.commit()

cursor.close()
conn.close()


if os.path.isfile('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/passport_blacklist_01032021.xlsx'):
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/passport_blacklist_01032021.xlsx',
          '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/passport_blacklist_01032021.xlsx.backup')
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/terminals_01032021.xlsx',
          '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/terminals_01032021.xlsx.backup')
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/transactions_01032021.txt',
          '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/transactions_01032021.txt.backup')

if os.path.isfile('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/passport_blacklist_02032021.xlsx'):
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/passport_blacklist_02032021.xlsx',
            '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/passport_blacklist_02032021.xlsx.backup')
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/terminals_02032021.xlsx',
            '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/terminals_02032021.xlsx.backup')
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/transactions_02032021.txt',
            '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/transactions_02032021.txt.backup')

if os.path.isfile('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/passport_blacklist_03032021.xlsx'):
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/passport_blacklist_03032021.xlsx',
            '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/passport_blacklist_03032021.xlsx.backup')
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/terminals_03032021.xlsx',
            '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/terminals_03032021.xlsx.backup')
    os.rename('/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/transactions_03032021.txt',
            '/Users/kalashnikova/Documents/универ/ВШЭ/2курс/1сем/DE/project/archive/transactions_03032021.txt.backup')
