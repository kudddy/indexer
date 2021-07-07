from time import sleep
import logging
# from Cache.cache import vacs_filename, index_filename
from plugins.uploader import Parser
from plugins.searchengine import InversIndexSearch
from plugins.helper import structure_normalization as norm
# пишем в базу
# from ext.pickler import Pickler as pcl
from plugins.config import cfg
# from Service.const import token_fastext, url_fasttext
# from Service.const import time_for_sleep_sheduller

from plugins.loader import vacs

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
    # заливаем в базу
    # pcl.dump_pickle_file(norm_d_vac, vacs_filename)
    # pcl.dump_pickle_file(SearchEngine.cache_index, index_filename)

    sleep(cfg.app.constant.time_for_sleep_sheduller)
