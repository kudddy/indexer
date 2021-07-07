import logging
from collections import Counter

from tqdm import tqdm

from plugins.tokenizer import QueryBuilder
# не нужен так как все в базе
# from ext.pickler import Pickler as pcl
from plugins.helper import get_clean_text_str
from plugins.fasttext import ClientFastText

# не нужен так как все в базе потому что serverless нахой
# from Cache.cache import index

tokenizer = QueryBuilder(out_clean='str', out_token='list')

index_filename = 'Cache/files/index_cache.p'

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

log.setLevel(logging.INFO)


class InversIndexSearch(ClientFastText):
    def __init__(self, url: str, topn=30):
        super(InversIndexSearch, self).__init__(url)
        self.topn = topn
        self.cache_index = {}

    def update_index(self, vacancy_dict: dict):
        self.cache_index['cache_index'] = {}
        self.cache_index['index_map'] = []
        global_cache = {}
        global_arr = []
        log.info('cleaning text')
        for k, dirty_string in tqdm(vacancy_dict.items()):
            clean_token = tokenizer.clean_query(get_clean_text_str(dirty_string['content']))
            global_cache.update({k: clean_token})
            global_arr.extend(clean_token)
        log.info('done cleaning text')

        log.info('indexing')
        for token in tqdm(set(global_arr)):
            # cоздание словаря с мэппингом слова к словам, которые близки по контексту
            result = [x[0] for x in self.get_most_similar(token, topn=self.topn)] + [token]
            size = self.topn + 1
            iter_dict = dict(zip(result, [token] * size))
            self.cache_index['index_map'].extend(list(iter_dict.items()))
            local_arr = []
            for k, v in global_cache.items():
                if token in v:
                    local_arr.append(k)
            self.cache_index['cache_index'].update({token: local_arr})
        log.info('done indexing')
        # TODO эти индексы мы должны залить
        # log.info('dump structure')
        # pcl.dump_pickle_file(self.cache_index, index_filename)

    def search(self, query: str, n=20) -> list:

        query = tokenizer.clean_query(query)
        result_dict = []
        for token in query:
            for tpl in self.cache_index['index_map']:
                if token == tpl[0]:
                    if tpl[1] in self.cache_index['cache_index'].keys():
                        result_dict.extend(self.cache_index['cache_index'][tpl[1]])

        cnt_object = Counter(result_dict)
        return sorted(cnt_object, key=cnt_object.get, reverse=True)[:n]
