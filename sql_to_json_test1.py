"""
CREATE PROC sp_data_juggler_test1 as
select
	'example' as [expamle],
	GETDATE() as [date],
	123.45 [Num],
	null as [main:]

select 'YT-12565' as DocNum,
	1 [doc_id:],
	convert(date,'04.05.2020') as DocDate,
	null as [:main]
union
select 'MR-4545' as DocNum,
	2 [doc_id:],
	convert(date,'05.05.2020') as DocDate,
	null as [:main]

select 1 [:doc_id],
	555666 [good_id],
	12 as [qnt]
union
select 1 [:doc_id],
	777888 [good_id],
	6 as [qnt]
union
select 2 [:doc_id],
	555666 [good_id],
	2 as [qnt]
union
select 2 [:doc_id],
	777888 [good_id],
	7 as [qnt]

"""

from settings import sql

from data_juggler import data_juggler

if __name__ == '__main__':
    source = "sqlserver://{0}:{1}@{2}/{3}/?".format(sql.login, sql.password,sql.server,sql.db)

    print(source)
    data_source = source + "data=sp_data_juggler_test1"
    dj = data_juggler(data_source)
    dj.join("data")
    print(dj.to_json("data"))

