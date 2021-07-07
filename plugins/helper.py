import re


def structure_normalization(d: list) -> dict:
    """
    Нормализация структуры данных, которая приходит из JobApi
    :param d:
    :return:
    """
    local_dict = {}
    for k in d:
        local_dict.update({k['id']: k})
    return local_dict


def remove_html_in_dict(text):
    html_pattern = re.compile('<.*?>')
    title_pattern = re.compile(r'([a-zа-я](?=[A-ZА-Я])|[A-ZА-Я](?=[A-ZА-Я][a-zа-я]))')

    val = title_pattern.sub(r'\1 ', html_pattern.sub(r'', text).replace('\xa0', ' '))
    text = re.sub(r'&[\w]*;', ' ', val).strip()
    return text


def get_clean_text_str(text_vacs: dict) -> str:
    """
    Ф-ция необходима для процедуры токенизации. Преобразует данные из словаря в единую строку
    :param text_vacs: словарь с описанием вакансии
    :return: единая строка содержащая в себе поля
    """
    if 'title' in text_vacs.keys():
        title = remove_html_in_dict(text_vacs['title'])
    else:
        title = 'fail'

    # обязанности
    if 'duties' in text_vacs.keys():
        duties_text = remove_html_in_dict(text_vacs['duties'])
    else:
        duties_text = 'fail'

    # условия
    if 'conditions' in text_vacs.keys():
        # conditions_text = remove_html_in_dict(text_vacs['conditions'])
        conditions_text = 'fail'
    else:
        conditions_text = 'fail'

    return title + ' ' + duties_text
