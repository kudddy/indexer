import asyncio
from time import sleep
import logging

from plugins.uploader import Parser
from plugins.searchengine import InversIndexSearch
from plugins.helper import structure_normalization as norm

from plugins.db.query import insert_data_index_map, \
    insert_data_cache_index, \
    insert_data_vacancy_content, \
    drop_cache_tables, \
    drop_main_tables
from plugins.config import cfg

SearchEngine = InversIndexSearch(url=cfg.app.hosts.fasttext)
get_data = Parser()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

log.setLevel(logging.INFO)

while True:
    log.info("Начало обновления")

    log.info("Выгрузка данных")
    d_vac = get_data.get_jobs_api(is_parse=False)
    norm_d_vac = norm(d_vac)

    log.info("Обновление индекса движка")

    SearchEngine.update_index(norm_d_vac)
    cache = SearchEngine.cache_index
    loop = asyncio.get_event_loop()

    # TODO сначала нужно дропнуть содержимое таблицы
    log.info("Очистка таблиц с кэшем")
    loop.run_until_complete(drop_cache_tables())

    log.info("загружаем расширенные индексы в базу")
    loop.run_until_complete(insert_data_index_map(cache))

    log.info("загружаем индексы в базу")
    loop.run_until_complete(insert_data_cache_index(cache))

    log.info("загружаем описанеи вакансий в базу")
    loop.run_until_complete(insert_data_vacancy_content(norm_d_vac))

    log.info("Заливаем содержимое кэша в основные таблицы")
    loop.run_until_complete(drop_main_tables())

    log.info("success finished")

    loop.close()
    sleep(cfg.app.constant.time_for_sleep_sheduller)
