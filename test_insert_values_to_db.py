import asyncio
import logging

from sqlalchemy import select, func, desc

from pathlib import Path
from asyncpgsa import PG
from tqdm import tqdm
from datetime import datetime
from plugins.loader import index_cache, vacs
from plugins.db.tables import index_map, cache_index, vacancy_content
from plugins.config import cfg

CENSORED = '***'
DEFAULT_PG_URL = cfg.app.hosts.pg
MAX_QUERY_ARGS = 32767
MAX_INTEGER = 2147483647

pg_pool_min_size = 10
pg_pool_max_size = 10
PROJECT_PATH = Path(__file__).parent.parent.resolve()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

log.setLevel(logging.INFO)

loop = asyncio.get_event_loop()


async def insert_data_index_map():
    pg = PG()
    await pg.init(
        str(DEFAULT_PG_URL),
        min_size=pg_pool_min_size,
        max_size=pg_pool_max_size
    )
    await pg.fetchval('SELECT 1')
    log.info('Connected to database %s', DEFAULT_PG_URL)

    for data in tqdm(index_cache['index_map']):
        query = index_map.insert().values(
            extended_index=data[0],
            original_index=data[1]
        )
        await pg.fetch(query)


asyncio.get_event_loop().run_until_complete(insert_data_index_map())


# async def drop_tables():
#     pg = PG()
#     await pg.init(
#         str(DEFAULT_PG_URL),
#         min_size=pg_pool_min_size,
#         max_size=pg_pool_max_size
#     )
#     await pg.fetch('')
#
#
# async def insert_data_cache_index():
#     pg = PG()
#     await pg.init(
#         str(DEFAULT_PG_URL),
#         min_size=pg_pool_min_size,
#         max_size=pg_pool_max_size
#     )
#     await pg.fetchval('SELECT 1')
#     log.info('Connected to database %s', DEFAULT_PG_URL)
#
#     for k, v in tqdm(index_cache['cache_index'].items()):
#         for data in v:
#             query = cache_index.insert().values(
#                 original_index=k,
#                 vacancy_id=data
#             )
#             await pg.fetch(query)
#
#
# import asyncpg
# import asyncio
#
#
# async def run():
#     con = await asyncpg.connect(user='postgres')
#     result = await con.copy_records_to_table(
#         'mytable', records=[
#             (1, 'foo', 'bar'),
#             (2, 'ham', 'spam')])
#     print(result)
#
#
# asyncio.get_event_loop().run_until_complete(run())
#
#
# async def insert_data_vacancy_content_test():
#     pg = PG()
#     await pg.init(
#         str(DEFAULT_PG_URL),
#         min_size=pg_pool_min_size,
#         max_size=pg_pool_max_size
#     )
#     await pg.fetchval('SELECT 1')
#     log.info('Connected to database %s', DEFAULT_PG_URL)
#
#     record: list = []
#
#     for k, v in tqdm(vacs.items()):
#         ids: int = k
#         title: str = v['content'].get('title', 'na')
#         footer: str = v['content'].get('footer', 'na')
#         header: str = v['content'].get('header', 'na')
#         requirements: str = v['content'].get('requirements', 'na')
#         duties: str = v['content'].get('duties', 'na')
#         conditions: str = v['content'].get('duties', 'na')
#         date = v['date']
#         date: datetime = datetime.strptime(date, '%Y-%m-%d')
#         locality: int = v.get('locality', 0)
#         region: int = v.get('region', 0)
#         company: int = v.get('company', 0)
#         record.append(
#             tuple([
#                 ids,
#                 title,
#                 footer,
#                 header,
#                 requirements,
#                 duties,
#                 conditions,
#                 date,
#                 locality,
#                 region,
#                 company
#             ])
#         )
#
#     vacancy_content.insert().values(record)
#
#
# async def insert_data_vacancy_content():
#     pg = PG()
#     await pg.init(
#         str(DEFAULT_PG_URL),
#         min_size=pg_pool_min_size,
#         max_size=pg_pool_max_size
#     )
#     await pg.fetchval('SELECT 1')
#     log.info('Connected to database %s', DEFAULT_PG_URL)
#
#     for k, v in tqdm(vacs.items()):
#         ids: int = k
#         title: str = v['content'].get('title', 'na')
#         footer: str = v['content'].get('footer', 'na')
#         header: str = v['content'].get('header', 'na')
#         requirements: str = v['content'].get('requirements', 'na')
#         duties: str = v['content'].get('duties', 'na')
#         conditions: str = v['content'].get('duties', 'na')
#         date = v['date']
#         date: datetime = datetime.strptime(date, '%Y-%m-%d')
#         locality: int = v.get('locality', 0)
#         region: int = v.get('region', 0)
#         company: int = v.get('company', 0)
#         query = vacancy_content.insert().values(
#             id=ids,
#             title=title,
#             footer=footer,
#             header=header,
#             requirements=requirements,
#             duties=duties,
#             conditions=conditions,
#             date=date,
#             locality=locality,
#             region=region,
#             company=company
#         )
#         await pg.fetch(query)
# loop.run_until_complete(give_me_vacancy())
# import json
#
#
# async def give_me_vacancy():
#     pg = PG()
#     await pg.init(
#         str(DEFAULT_PG_URL),
#         min_size=pg_pool_min_size,
#         max_size=pg_pool_max_size
#     )
#     await pg.fetchval('SELECT 1')
#     log.info('Connected to database %s', DEFAULT_PG_URL)
#
#     """select cache_index.vacancy_id, count(cache_index.vacancy_id) as counter from (select original_index from index_map
# where extended_index = 'django') as times
# join cache_index on times.original_index = cache_index.original_index
# group by cache_index.vacancy_id
# order by counter desc"""
#
#     # query = select([comments.c.file, func.count(comments.c.file).label('count')]) \
#     #     .group_by(comments.c.file) \
#     #     .order_by(desc('count'))
#
#     times = select([index_map.c.original_index]).where(index_map.c.extended_index == "python").alias("times")
#
#     j = times.join(cache_index, times.c.original_index == cache_index.c.original_index)
#
#     result = select([cache_index.c.vacancy_id, func.count(cache_index.c.vacancy_id).label('counter')]).select_from(j)
#
#     search_result = result.group_by(cache_index.c.vacancy_id).order_by(desc('counter')).alias("search_result")
#
#     """select search_result.*, vacancy_content.* from (select cache_index.vacancy_id, count(cache_index.vacancy_id) as counter from (select original_index from index_map
# where extended_index = 'python') as times
# join cache_index on times.original_index = cache_index.original_index
# group by cache_index.vacancy_id) as search_result
#
# join vacancy_content on search_result.vacancy_id = vacancy_content.id
#
# order by search_result.counter desc
#
# limit 30"""
#
#     # жоин поискового результата с таблицей описания вакансий
#
#     j = search_result.join(vacancy_content, search_result.c.vacancy_id == vacancy_content.c.id)
#
#     query = select([search_result, vacancy_content]).select_from(j).order_by(desc(search_result.c.counter)).limit(30)
#
#     ready_content = []
#     columns = ["id", "title", "footer", "header", "requirements", "duties", "conditions", "date", "locality", "region",
#                "company"]
#     for row in await pg.fetch(query):
#         reconstruction: dict = {v: row[v] for v in columns}
#
#         ready_content.append(reconstruction)
#
#     for column in columns:
#         # print(str(ready_content[0][column]))
#         # print(type(str(ready_content[0][column])))
#         try:
#             json.dumps(str(ready_content[0][column]))
#         except:
#             print(str(ready_content[0][column]))
#             print(type(str(ready_content[0][column])))
#
#
# loop.run_until_complete(give_me_vacancy())
