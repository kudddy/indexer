import re
import requests
from tqdm.auto import trange
import pandas as pd

from plugins.loader import regions, locality, category


class Parser:
    def __init__(self):
        self.space_pattern = re.compile(r'[^.А-ЯA-ZЁ0-9]+', re.I)
        self.tag_pattern = re.compile(r'&[\w]*;')
        self.html_pattern = re.compile('<.*?>')
        self.title_pattern = re.compile(r'([a-zа-я](?=[A-ZА-Я])|[A-ZА-Я](?=[A-ZА-Я][a-zа-я]))')
        self.sentence_pattern = re.compile(r'!?;.')
        self.regions = regions
        self.locality = locality
        self.category = category

    def get_jobs_api(self, num_vacs=200, is_parse=True):
        site_url = 'https://my.sbertalents.ru/'
        self._get_input_type('api')
        jobs = []
        i = 0
        my_json = requests.get(site_url + f'job-requisition/v3?page={i}&size={num_vacs}', verify=False).json()
        for i in trange(1, my_json['totalElements'] // num_vacs + 2):
            jobs.extend(my_json['content'])
            my_json = requests.get(site_url + f'job-requisition/v3?page={i}&size={num_vacs}', verify=False).json()
        if is_parse:
            jobs = self.parse_jobs(jobs)
        return jobs

    def _get_input_type(self, input_type):
        assert input_type in ['api', 'filesystem']

        if input_type == 'filesystem':
            self.content_column = 'Text_Job'
            self.title_column = 'jobTitle'
            self.description_columns = ['external_text']
            self.get_id = lambda job: job['Text_Job']['key']
        else:
            self.content_column = 'content'
            self.title_column = 'title'
            self.description_columns = ['requirements', 'duties']
            self.get_id = lambda job: job['id']

    def parse_jobs(self, dirty_jobs, input_type='api', to_dataframe=True):
        self._get_input_type(input_type)
        jobs = {}
        if input_type == 'filesystem':
            dirty_jobs = dirty_jobs.values()
        for job in dirty_jobs:
            content = job[self.content_column]
            if self.title_column in content:
                title = content[self.title_column]
                description = ''
                for column in self.description_columns:
                    if column in content:
                        description += self._remove_html(content[column])
                if description and title:
                    jobs[int(self.get_id(job))] = {
                        'title': title,
                        'description': description,
                    }
        if input_type == 'api':
            for job in dirty_jobs:
                uid = int(self.get_id(job))
                if uid in jobs:
                    address = ''
                    if self.locality.get(job.get('locality')):
                        address += self.locality.get(job.get('locality')) + ' '
                    if self.regions.get(job.get('region')):
                        address += self.regions.get(job.get('region'))
                    address = address if address.strip() else None
                    category = -1
                    if job.get('postingCategory') in self.category:
                        category = self.category[job.get('postingCategory')]
                    jobs[uid].update({'category': category,
                                      'address': address})
        if to_dataframe:
            return pd.DataFrame(jobs).T
        else:
            new_jobs = {k: {'title': v['title'], 'description': v['description']} for k, v in jobs.items()}
            return new_jobs

    def _remove_html(self, line):
        result_line = self.title_pattern.sub(r'\1 ', self.html_pattern.sub(r' ', line).replace('\xa0', ' '))
        result_line = self.space_pattern.sub(' ', self.sentence_pattern.sub(
            '.', self.tag_pattern.sub(' ', result_line))).strip()
        return result_line

    def parse_resumes(self, dirty_resumes):
        resumes = pd.DataFrame(dirty_resumes).T
        return resumes

    def targets_prepare(self, targets):
        return {int(k): {int(k2): 0 if v2 == 'Disqualified' else 1 for k2, v2 in v['status'].items()}
                for k, v in targets.items()}
