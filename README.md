## Типовые операции с таблицей
переименование таблицы
```
alter table vacancy_info rename to vacancy_content;
```
поисковой запрос к базе
```
select cache_index.vacancy_id, count(cache_index.vacancy_id) as counter from (select original_index from index_map
where extended_index = 'django') as times
join cache_index on times.original_index = cache_index.original_index
group by cache_index.vacancy_id
order by counter desc
```
как залить рекорд в таблицу
```
import asyncpg
import asyncio
async def run():
     con = await asyncpg.connect(user='postgres')
     result = await con.copy_records_to_table(
         'mytable', records=[
             (1, 'foo', 'bar'),
             (2, 'ham', 'spam')])
     print(result)
asyncio.get_event_loop().run_until_complete(run())
```
как создать копию таблицы без данных
```
create table dupe_vacancy_content as (select * from cache_index) with no data
```
как создать копию таблицы с данными
```
create table dupe_vacancy_content as (select * from cache_index)
```
переименование таблицы
```
alter table cache_index rename to cache_inde;
```