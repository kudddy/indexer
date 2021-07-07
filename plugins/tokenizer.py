import re
import pymorphy2
import stop_words
from nltk.tokenize import RegexpTokenizer

STOP_WORDS = ['кандидат', 'собеседовать', 'вакансия', 'найм']


class TransformSentences:
    """
    Класс дает возможность трансформировать словосочетание, а именно чистить предложения от пунктуации и прочего,
    и стандартизировать слова в предложении
        type(int) - n : чистить ли от слов с числом букв меньне n
        type(str) - sep : разделитей который нужен на выходе
        type(bool) - arr_out : на выходе список или нет
    """

    def __init__(self, n=0, sep=' ', out_clean='str', out_token='str'):
        # сколько на выходе похожих объектов
        self.__n = n
        # разделитель
        self.__sep = sep
        self.__out_clean = out_clean
        self.__out_token = out_token
        self.__morph = pymorphy2.MorphAnalyzer()
        self.__tokenizer = RegexpTokenizer('[A-Za-zА-Яа-яёЁ]+')

        self.list_stop = stop_words.get_stop_words(language='russian') + STOP_WORDS

    '''
    Чистим слова от мусора и удаляем стоп слова
    Аrgs:
        type(str) - text : словосочетание
    Returns:
        type(str or list) - lemms : чистые слова
    '''

    def tokenize(self, txt: str):
        tokens = self.__tokenizer.tokenize(txt)
        lemms = []
        for word in tokens:
            if len(word) >= 2:
                p = self.__morph.parse(word)[0]
                np = p.normal_form
                lemms.append(np)
        if self.__out_token is 'str':
            return self.__sep.join([x for x in lemms if x not in self.list_stop])
        elif self.__out_token is 'list':
            return [x for x in lemms if x not in self.list_stop]

    '''
    Токенизатор. Стандартизируем слова
    Аrgs:
        type(str) - text : словосочетание
    Returns:
        type(str or list) - text : чистые слова
    '''

    def clean_word(self, txt: str):
        txt = re.compile('[^a-zа-яA-ZА-Я ]').sub(' ', str(txt))
        text = txt.rstrip().lstrip().lower()
        text = [x for x in text.split(' ') if x not in self.list_stop and len(x) > self.__n]
        if self.__out_clean is 'str':
            return self.__sep.join(text)
        elif self.__out_clean is 'list':
            return text


class QueryBuilder(TransformSentences):
    def __init__(self, n=0, sep=' ', out_clean='str', out_token='list'):
        super().__init__(n, sep, out_clean, out_token)

    def clean_query(self, query: str):
        query = self.clean_word(query)
        query = self.tokenize(query)
        return query
