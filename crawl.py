# coding=utf-8
import sys
import requests, json, os, re, datetime, traceback, time
import threading
from urllib import unquote
from pymongo import MongoClient
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')


class Crawl(threading.Thread):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'xsrf=c0fe057e-98f5-4471-b231-692c64230cf4; __utma=51854390.10558487.1507263364.1507263364.1508598619.2; __utmc=51854390; __utmv=51854390.100--|2=registration_date=20150117=1^3=entry_date=20150117=1; __utmz=51854390.1508598619.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; aliyungf_tc=AQAAAA29X30YRwkAW2wKb0pgkAA5YVpk; z_c0=Mi4xU0tqbEFBQUFBQUFBa0lLLUdRMThEQmNBQUFCaEFsVk5xSkQtV1FCRjVzM01xejhDczNJOEdkanRiY1FJZjhfUTl3|1507263400|9891c5b82195e758293d8d6f2e8066f08bff08ed; d_c0="AJCCvhkNfAyPTqimB3ACOh9sDVSQqeu8il8=|1507263363"; cap_id="YjcyYzlkYzZlODQxNDQzN2E2NzA5NGI4YTcxMTBhOGY=|1507263362|e7fc7743e1e36255057f6b66e1431bc991ad2bd7"; l_cap_id="OTY2MGI0NWIxNWZmNGEyYzgwYTk1Y2E1OTQ2MDk3OTI=|1507263362|e7f8a86ebcbdde0944d53ae59bbbc53ba2f058bb"; r_cap_id="MDJiMjZhNmE4NDEwNGM3NjhkMzA2OTE0ZjllMjNhZjY=|1507263362|acc214be190644802808fa66a694d118927d9f47"; _zap=1393a095-7d03-44e9-a502-34a180cac5a6; q_c1=210770d03d984b88903544c95535d56e|1507185102000|1507185102000; q_c1=9f3045961e7c48e0a58297ce036b72f7|1507185102000|1507185102000',
        'Host': 'www.zhihu.com',
        'Referer': 'https://www.zhihu.com/oauth/callback/sina?state=62373239323333612d396163632d343661382d616564312d613363666563643135313338&code=cd8dec1a55bea4be7179c5347435d7bb',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    }

    mongo_host = '127.0.0.1'
    mongo_port = '27017'
    dbname = 'zhihu'

    data_path = './data/'

    page_offset = 0
    page_limit = 20
    follow_url = 'https://www.zhihu.com/api/v4/members/$uid/followees?include=' \
                    'data[*].answer_count,articles_count,gender,follower_count,is_followed,' \
                    'is_following,badge[?(type=best_answerer)].topics&offset=$offset&limit=$limit'

    followers_url = 'https://www.zhihu.com/api/v4/members/$uid/followers?include=' \
                    'data[*].answer_count,articles_count,gender,follower_count,is_followed,' \
                    'is_following,badge[?(type=best_answerer)].topics&offset=$offset&limit=$limit'

    answers_url = 'https://www.zhihu.com/api/v4/members/$uid/answers?include=' \
                  'data[*].is_normal,is_collapsed,annotation_action,annotation_detail,' \
                  'collapse_reason,collapsed_by,suggest_edit,comment_count,can_comment,' \
                  'content,voteup_count,reshipment_settings,comment_permission,mark_infos,' \
                  'created_time,updated_time,review_info,relationship.is_authorized,voting,' \
                  'is_author,is_thanked,is_nothelp,upvoted_followees;' \
                  'data[*].author.badge[?(type=best_answerer)].topics&offset=$offset&limit=$limit&sort_by=created'

    def __init__(self, cookie=''):
        if cookie:
            self.headers['Cookie'] = cookie
        try:
            mongo = MongoClient('mongodb://' + self.mongo_host + ':' + self.mongo_port)
            self.db = getattr(mongo, self.dbname)
        except Exception, e:
            self.log('mongo-connect-error', e)
            raise e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            ex = traceback.extract_tb(exc_tb)
            print '错误：'
            print type(exc_val)
            self.log('exception', str(exc_type) + ': ' + str(exc_val) + '\n' + '\n'.join([', '.join(map(str, li)) for li in ex]))
            return True

    @staticmethod
    def mkfile(filename, content):
        paths = filename.split('/')
        path = ''
        for i in range(len(paths) - 1):
            path += paths[i] + '/'
            if os.path.exists(path) is False:
                os.mkdir(path)
        with open(filename, 'a') as f:
            f.write(content)

    def log(self, name, info):
        content = '''
\n◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆◇◆
%(time)s
%(info)s
        ''' % {'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
               'func': sys._getframe().f_code.co_name, 'back_func': sys._getframe().f_back.f_code.co_name,
               'info': info}
        self.mkfile('./log/' + name + '.log', content)

    def save_mm_answers(self, uid):
        mm_answers = self.db.mm_answers.find_one({'_id': uid})
        if mm_answers:
            print '    ', mm_answers['name'], 'is exist'
            return False
        url = self.answers_url.replace('$uid', uid)
        data = self.get_all_pages_data(url)
        for k, v in enumerate(data['data']):
            pt = re.compile('<img.*?src="((?:(?!zhstatic|image\/svg).)*?)"', re.I)
            pics = re.findall(pt, v['content'])
            if pics:
                data['data'][k]['answer_pics'] = pics
            else:
                del data['data'][k]
        data['_id'] = uid
        data['status'] = 'unmined'
        data.update(data['data'][0]['author'])
        # try:
        #     result = self.db.mm_answers.insert_one(data)
        #     return result.inserted_id
        # except Exception, e:
        #     print e
        result = self.db.mm_answers.save(data)
        return result

    def save_mm_answers_pics(self, uid, answers=None):
        if answers is None:
            # answers = self.db.mm_answers.find_one({'_id': uid})
            url = self.answers_url.replace('$uid', uid)
            answers = self.get_all_pages_data(url)
        mm_pics = []
        for v in answers['data']:
            pt = re.compile('<img.*?src="((?:(?!zhstatic|image\/svg).)*?)"', re.I)
            pics = re.findall(pt, v['content'])
            if pics:

                mm_pics.append({
                    '_id': v['id'],
                    'uid': v['author']['url_token'],
                    'name': v['author']['name'],
                    'avatar': v['author']['avatar_url_template'],
                    'url': v['question']['url'],
                    'question': v['question']['title'],
                    'question_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(v['question']['created'])),
                    'answer': v['content'],
                    'answer_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(v['created_time'])),
                    'pics': pics
                })
        if mm_pics:
            self.db.mm_answers_pics.insert_many(mm_pics)
        return len(mm_pics)

    def save_mm_followers(self, uid, male_uid):
        mm_followers = self.db.mm_followers.find_one({'_id': uid})
        if mm_followers:
            print '    ', mm_followers['_id'], 'is exist'
            return False
        url = self.followers_url.replace('$uid', uid)
        data = self.get_all_pages_data(url)
        data.update({'data': [v for v in data['data'] if v['gender'] is 1 and v['url_token'] != male_uid]})
        data['_id'] = uid
        data['status'] = 'unmined'
        result = self.db.mm_followers.save(data)
        return result

    def save_male_follow(self, uid, mm_uid=None):
        male_follow = self.db.male_follow.find_one({'_id': uid})
        if male_follow:
            print '    ', male_follow['_id'], 'is exist'
            return False
        url = self.follow_url.replace('$uid', uid)
        data = self.get_all_pages_data(url)
        data.update({'data': [v for v in data['data'] if v['gender'] is 0 and v['follower_count'] > 10 and v['url_token'] != mm_uid]})
        data['_id'] = uid
        data['status'] = 'unmined'
        result = self.db.male_follow.save(data)
        return result

    def get_all_pages_data(self, url, page_offset=None, page_limit=None):
        if page_offset is None:
            page_offset = self.page_offset
        if page_limit is None:
            page_limit = self.page_limit
        request_url = unquote(url).replace('$offset', str(page_offset)).replace('$limit', str(page_limit))
        r = requests.get(request_url, headers=self.headers)
        if r.status_code != 200:
            self.log('request-error', r.status_code)
            return False
        data = json.loads(r.text)
        if 'errmsg' in data:
            self.log('request-error', data['errmsg'])
            print data['errmsg']
        print data['paging']['totals'], page_offset
        if data['paging']['is_end'] is not True:
            next_data = self.get_all_pages_data(url, page_offset + page_limit, page_limit)
            data['data'] += next_data['data']
            return data
        else:
            return data


def dump(data):
    print json.dumps(data, indent=4, ensure_ascii=False)


class CrawlGo(threading.Thread):

    def __init__(self, action):
        threading.Thread.__init__(self)
        self.action = getattr(self, action)

    def run(self):
        self.action()

    # 处理mm, 处理用户关注的mm
    def followed_mm(self):
        with Crawl() as crawl:
            # 保存初始用户关注的mm
            crawl.save_male_follow('chen-hao-44-41')
            while 1:
                print '\nstart male_follow ...'
                male_follow = list(crawl.db.male_follow.find({'status': 'unmined'}))
                for male in male_follow:
                    crawl.db.male_follow.update({'_id': male['_id']}, {'$set': {'status': 'mining'}})
                    print '\n  ', male['_id'], ' ...'
                    for mm in male['data']:
                        # 保存mm的回答及回答图片
                        print '\n    ', mm['name'], 'save answers ...'
                        #crawl.save_mm_answers(mm['url_token'])
                        save_mm_answers = threading.Thread(target=crawl.save_mm_answers, args=(mm['url_token'],))
                        save_mm_answers.start()
                        # 保存关注mm的用户
                        print '    ', mm['name'], 'save followers ...'
                        #crawl.save_mm_followers(mm['url_token'], male['_id'])
                        save_mm_followers_thread = threading.Thread(target=crawl.save_mm_followers, args=(mm['url_token'], male['_id']))
                        save_mm_followers_thread.start()
                    crawl.db.male_follow.update({'_id': male['_id']}, {'$set': {'status': 'mined'}})
                print '\nend male_follow ...'
                time.sleep(1)

    # 处理用户, 处理mm的关注者用户
    def follow_male(self):
        with Crawl() as crawl:
            while 1:
                print '\nstart mm_followers ...'
                mm_followers = list(crawl.db.mm_followers.find({'status': 'unmined'}))
                for mm in mm_followers:
                    crawl.db.male_follow.update({'_id': male['_id']}, {'$set': {'status': 'mining'}})
                    print '\n  ', mm['_id'], ' ...'
                    for male in mm['data']:
                        # 保存用户关注的mm
                        print '\n    ', male['name'], 'save follow ...'
                        crawl.save_male_follow(male['url_token'], mm['_id'])
                    crawl.db.mm_followers.update({'_id': mm['_id']}, {'$set': {'status': 'mined'}})
                print '\nend mm_followers ...'
                time.sleep(1)


if __name__ == '__main__':
    followed_mm = CrawlGo('followed_mm')
    followed_mm.start()
    follow_male = CrawlGo('follow_male')
    follow_male.start()
    print '....................over........................'