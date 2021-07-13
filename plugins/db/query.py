import logging

from pathlib import Path
from asyncpgsa import PG
from tqdm import tqdm
from datetime import datetime
from plugins.db.tables import index_map, cache_index, vacancy_content
from plugins.helper import chunks
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


async def drop_cache_tables():
    pg = PG()
    await pg.init(
        str(DEFAULT_PG_URL),
        min_size=pg_pool_min_size,
        max_size=pg_pool_max_size
    )

    await pg.fetchval('TRUNCATE dupe_vacancy_content')
    await pg.fetchval('TRUNCATE dupe_index_map')
    await pg.fetchval('TRUNCATE dupe_cache_index')

    log.info('tables clean')


async def drop_main_tables():
    pg = PG()
    await pg.init(
        str(DEFAULT_PG_URL),
        min_size=pg_pool_min_size,
        max_size=pg_pool_max_size
    )

    await pg.fetchval('drop table cache_index;')
    await pg.fetchval('drop table index_map;')
    await pg.fetchval('drop table vacancy_content;')

    log.info('tables cleant %s', DEFAULT_PG_URL)

    await pg.fetchval('create table index_map as (select * from dupe_index_map);')
    await pg.fetchval('create table vacancy_content as (select * from dupe_vacancy_content);')
    await pg.fetchval('create table cache_index as (select * from dupe_cache_index);')


async def insert_data_index_map(cache: dict):
    pg = PG()
    await pg.init(
        str(DEFAULT_PG_URL),
        min_size=pg_pool_min_size,
        max_size=pg_pool_max_size
    )
    await pg.fetchval('SELECT 1')
    log.info('Connected to database %s', DEFAULT_PG_URL)

    for chunk in tqdm(chunks(cache['index_map'], 1000)):
        values = []
        for data in chunk:
            values.append(
                {
                    'extended_index': data[0],
                    'original_index': data[1]
                }
            )
        query = index_map.insert().values(values)
        await pg.fetch(query)

    # for data in tqdm(cache['index_map']):
    #     query = index_map.insert().values(
    #         extended_index=data[0],
    #         original_index=data[1]
    #     )
    #     await pg.fetch(query)


async def insert_data_cache_index(cache: dict):
    pg = PG()
    await pg.init(
        str(DEFAULT_PG_URL),
        min_size=pg_pool_min_size,
        max_size=pg_pool_max_size
    )
    await pg.fetchval('SELECT 1')
    log.info('Connected to database %s', DEFAULT_PG_URL)

    values = []
    for k, v in tqdm(cache['cache_index'].items()):
        for data in v:
            values.append(
                {
                    'original_index': k,
                    'vacancy_id': data
                }
            )

    for chunk in chunks(values, 1000):
        query = cache_index.insert().values(chunk)
        await pg.fetch(query)


async def insert_data_vacancy_content(vac: dict):
    pg = PG()
    await pg.init(
        str(DEFAULT_PG_URL),
        min_size=pg_pool_min_size,
        max_size=pg_pool_max_size
    )
    await pg.fetchval('SELECT 1')
    log.info('Connected to database %s', DEFAULT_PG_URL)
    values = []
    for k, v in tqdm(vac.items()):
        values.append(
            {
                'id': k,
                'title': v['content'].get('title', 'na'),
                'footer': v['content'].get('footer', 'na'),
                'header': v['content'].get('header', 'na'),
                'requirements': v['content'].get('requirements', 'na'),
                'duties': v['content'].get('duties', 'na'),
                'conditions': v['content'].get('duties', 'na'),
                'date': datetime.strptime(v['date'], '%Y-%m-%d'),
                'locality': v.get('locality', 0),
                'region': v.get('region', 0),
                'company': v.get('company', 0)
            }
        )
    for chunk in chunks(values, 1000):
        query = vacancy_content.insert().values(chunk)
        await pg.fetch(query)
