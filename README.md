# 1.data_juggler
Functions for converting and exchanging data
## Sql to JSON
### Example 1: 
#### Create next proc in your MSSQL database:
```sql
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

```
####  Run sql_to_json_test1.py

```python
from data_juggler import data_juggler

if __name__ == '__main__':
    source = "sqlserver://login:pass@server/base/?"
    data_source = source + "data=sp_data_juggler_test1"
    dj = data_juggler.data_juggler(data_source)
    dj.join("data")
    print(dj.to_json("data"))
```

####  See result:
```json
{
  "expamle": "example",
  "date": "07.05.2020 12:13:15",
  "Num": 123.45,
  "main": [
    {
      "DocNum": "YT-12565",
      "DocDate": "04.05.2020",
      "doc_id": [
        {
          "good_id": 555666,
          "qnt": 12
        },
        {
          "good_id": 777888,
          "qnt": 6
        }
      ]
    },
    {
      "DocNum": "MR-4545",
      "DocDate": "05.05.2020",
      "doc_id": [
        {
          "good_id": 555666,
          "qnt": 2
        },
        {
          "good_id": 777888,
          "qnt": 7
        }
      ]
    }
  ]
}
```
### Example 2: soon

# 2.spryreport.py
## JSON to mustache XLSX (spryreport.py)
#### description soon



