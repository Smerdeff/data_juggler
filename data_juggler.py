import traceback
import logging
from logging.handlers import BufferingHandler
from json import encoder
import itertools
# import paramiko
# import pysftp
# import xmltodict
import spryreport
import zipfile
import io
import sys
import os
import csv
import ast
import pyodbc
import json
import argparse
import datetime
import decimal
from urllib import request, error
from urllib.parse import quote, urlsplit, urlunsplit, parse_qsl, urlencode, urlparse, parse_qs, unquote
import configparser
import smtplib
# Добавляем необходимые подклассы - MIME-типы
import mimetypes  # Импорт класса для обработки неизвестных MIME-типов, базирующихся на расширении файла
from email import encoders  # Импортируем энкодер
from email.mime.base import MIMEBase  # Общий тип
from email.mime.text import MIMEText  # Текст/HTML
from email.mime.image import MIMEImage  # Изображения
from email.mime.audio import MIMEAudio  # Аудио
from email.mime.multipart import MIMEMultipart  # Многокомпонентный объект
# from email.mime.e
# import django_pyodbc


import gzip as gzip_lib


# from collections import defaultdict
# import operator


class data_juggler_base:
    sqlserver = 'sqlserver'
    mysql = 'mysql'
    postgresql = 'postgresql'
    sqlserver_default_scheme = 'dbo'
    query_get_stored_parameters = 'select PARAMETER_NAME from information_schema.PARAMETERS where SPECIFIC_CATALOG=? and SPECIFIC_SCHEMA=? and SPECIFIC_NAME=?'

    @staticmethod
    def _dequote(s):
        if (s[0] == s[-1]) and s.startswith(("'", '"')):
            return s[1:-1]
        return s

    def get_sqlserver_port(self):
        return self._sqlserver_port

    def set_sqlserver_port(self, value):
        self._sqlserver_port = value

    def del_sqlserver_port(self):
        del self._sqlserver_port

    sqlserver_port = property(get_sqlserver_port, set_sqlserver_port, del_sqlserver_port, 'sqlserver default port')

    def get_auto_commit(self):
        return self._auto_commit

    def set_auto_commit(self, value):
        self._auto_commit = value

    def del_auto_commit(self):
        del self._auto_commit

    auto_commit = property(get_auto_commit, set_auto_commit, del_auto_commit, 'auto commit')

    def get_sqlserver_driver(self):
        return self._sqlserver_driver

    def set_sqlserver_driver(self, value):
        self._sqlserver_driver = value

    def del_sqlserver_driver(self):
        del self._sqlserver_driver

    sqlserver_driver = property(get_sqlserver_driver, set_sqlserver_driver, del_sqlserver_driver, 'sqlserver driver')

    def get_db(self):
        return self._db

    def set_db(self, value):
        self._db = value

    def del_db(self):
        del self._db

    db = property(get_db, set_db, del_db, 'db')

class data_juggler(data_juggler_base):
    def connect(self, scheme, server=None, database=None, driver=None, port=None, username=None, password=None):
        if scheme in ['sqlserver']:
            if driver is None:
                driver = self._sqlserver_driver
            if port is None:
                port = self._sqlserver_port
            # print(scheme, server, database, driver, port, username, password)
            conn_str = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=' + str(port) + ';DATABASE=' + database + ';'
            if username is None:
                conn_str = conn_str + 'Trusted_Connection=yes;'
            else:
                conn_str = conn_str + 'UID=' + username + ';PWD=' + password + ';'
            # print(conn_str)
            self._db = pyodbc.connect(conn_str, autocommit=self.auto_commit)
            self._db_option = (scheme, server, database, driver, port, username, password)
            return True
        if scheme in ['sqlite', 'sqlite3']:
            if driver is None:
                driver = self._sqlite_driver
            conn_str = 'DRIVER=' + driver + ';DATABASE=' + database + ';'
            self._db = pyodbc.connect(conn_str, autocommit=self.auto_commit)
            self._db_option = (scheme, server, database, driver, port, username, password)
            return True


        return False

    def url(self, url):
        url_p = urlparse(url, False)
        # print(url_p)
        if url_p.scheme in ['sqlserver']:
            database = url_p.path.split("/")[1]  # TODO Переделать с учетом инстанса
            server = url_p.hostname
            if database is not None and server is not None:
                self.connect(scheme=url_p.scheme, server=server, database=database, username=url_p.username, password=url_p.password)
        if self._db:
            for name, query in parse_qsl(url_p.query):
                self.open(query_name=name, query=self._dequote(query))
        return

    def __init__(self, url=None, **kwargs):
        self._db = None
        self._sqlserver_port = 1433
        self._auto_commit = True
        self._sqlserver_driver = '{SQL Server Native Client 11.0}'
        self._sqlite_driver = 'SQLite3 ODBC Driver'
        self.__dict__.update(kwargs)
        self.data = {}

        if url is not None:
            self.url(url=url)
        return


    def _format_value(self, value):
        # print(value, type(value))
        if value is None:
            return ''
        elif isinstance(value, str):
            return value
        elif isinstance(value, int):
            return value
        elif isinstance(value, datetime.datetime):
            return value.strftime("%d.%m.%Y %H:%M:%S")
        elif isinstance(value, datetime.date):
            return value.strftime("%d.%m.%Y")
        elif isinstance(value, decimal.Decimal):
            return float(value)
        return str(value)

    # class dj_JSONEncoder(json.JSONEncoder):
    #     # def __init__(self, *args, **kwargs):
    #     #     super().__init__(*args, **kwargs)
    #     #     self.indentation_level = 0
    #
    #     def default(self, value):
    #         if isinstance(value, datetime.datetime):
    #             return value.strftime("%d.%m.%Y %H:%M:%S")
    #         elif isinstance(value, datetime.date):
    #             return value.strftime("%d.%m.%Y")
    #         elif isinstance(value, decimal.Decimal):
    #             # print(float(value))
    #             return float(value)
    #         return json.JSONEncoder.default(self,  value)
        #
        # def encode(self, value):
        #     value[0]['vidprice'][0]['null_string']=''
        #     return json.dumps(value,ensure_ascii=self.ensure_ascii, default=self.default)

    def join(self, query_name, free_reference=True):
        """

        :param query_name:
        :param free_reference:
        :return:
        """
        def join_filter(f_set, key, value):
            for row in f_set:
                if key in row:
                    if row[key] == value:
                        yield row

        for i, u_set in enumerate(self.data[query_name]):
            if u_set is not None:
                if len(self.data[query_name]) > (i + 1):  # Режим состыковки датасетов
                    for row in u_set:  # Побежали по строкам
                        for key in row:  # Побежали по ключам в строке
                            # TODO переделать на возможность использовать символ ":" в названии через "\"
                            if key[-1:] == ':':  # Ищем с окончанием на ":"
                                filter_value = row[key]
                                # print(key, filter_value)

                                row[key] = []
                                for f_set in self.data[query_name][i + 1:]:  # Перебираем все датасеты, кроме основного.
                                    if f_set:
                                        # Присвоили подходяхий список из датасета в основной датасет текущей строоке
                                        row[key] += list(
                                            join_filter(f_set, ':' + key[:-1], filter_value)
                                        )
                                row[key[:-1]] = row.pop(key)


        # Чистим ссылки
        if free_reference:
            for dataset in self.data[query_name]:
                if dataset:
                    for row in dataset:
                        for key in row.copy():
                            if key[0:1] == ':':
                                row.pop(key, None)
        return True

    def to_json(self, query_name):
        # return json.dumps(self.data[query_name][0], ensure_ascii=False, cls=self.dj_JSONEncoder)
        return json.dumps(self.data[query_name][0][0], ensure_ascii=False, default=self._format_value)


    def _get_named_list(self, cur):
        rows = cur.fetchall()
        if rows:
            return [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in rows]

    def _stored_get_params(self, stored):
        name_spl = stored.split('.')[::-1]
        catalog = schema = name = None
        if len(name_spl) > 0: name = name_spl[0]
        if len(name_spl) > 1: schema = name_spl[1]
        if len(name_spl) > 2: catalog = name_spl[2]
        if self._db_option[0] == self.sqlserver:
            if not name: return None
            if not schema: schema = self.sqlserver_default_scheme
            if not catalog: catalog = self._db_option[2]
        sql = self.query_get_stored_parameters
        data = {}
        self.open(query=sql, params=(catalog, schema, name), data=data)
        for row in data[None][0]:
            yield row['PARAMETER_NAME']

    def open_stored(self, stored, params=None, query_name=None, data=None, db=None):
        def pure_param(z):
            if z[0] == '@':
                return z[1:]
            return z

        def compare_params(a, b):
            for i in list(itertools.product(a, b)):
                if pure_param(i[0]) == pure_param(i[1]):
                    yield i
        if stored:
            p_values = []
            query = stored + ' '
            for i, p in enumerate(compare_params(self._stored_get_params(stored), params.keys())):
                if i > 0: query += ','
                query += p[0] + '=?'
                p_values.append(params[p[1]])
            self.open(query=query, params=p_values, query_name=query_name, data=data, db=db)
        return

    def open(self, query, params=(), query_name=None, data=None, db=None):
        if not db: db = self._db
        if not isinstance(data, dict): data = self.data
        cur = db.cursor()
        # print(params)
        cur.execute(query, params)
        if data.get(query_name):
            data[query_name].append(self._get_named_list(cur))
        else:
            data[query_name] = [self._get_named_list(cur)]
        while cur.nextset():
            data[query_name].append(self._get_named_list(cur))

        cur.close()
        return True


def install_opener(baseurl, login, password):
    password_mgr = request.HTTPPasswordMgrWithDefaultRealm()
    # Add the username and password.
    # If we knew the realm, we could use it instead of None.
    password_mgr.add_password(None, baseurl, login, password)
    handler = request.HTTPBasicAuthHandler(password_mgr)
    # create "opener" (OpenerDirector instance)
    opener = request.build_opener(handler)
    # use the opener to fetch a URL
    opener.open(baseurl)
    # Install the opener.
    # Now all calls to urllib.request.urlopen use our opener.
    request.install_opener(opener)
    return


def send_email(addr_to, msg_subj, msg_text, content=None, ContentFileName=None):
    config = configparser.ConfigParser()
    config.sections()
    config_path = os.path.abspath(os.path.dirname(__file__)) + '/data_juggler.ini'
    config.read(config_path)

    # config.read('data_juggler.ini')

    addr_from = config['email']['HOST_USER']  # Отправитель
    password = config['email']['HOST_PASSWORD']
    HOST = config['email']['HOST']
    PORT = config['email']['PORT']
    USE_TLS = config['email']['USE_TLS']

    msg = MIMEMultipart()  # Создаем сообщение
    msg['From'] = addr_from  # Адресат
    msg['To'] = addr_to  # Получатель
    msg['Subject'] = msg_subj  # Тема сообщения

    body = msg_text  # Текст сообщения
    msg.attach(MIMEText(body, 'plain'))  # Добавляем в сообщение текст

    # process_attachement(msg, files)
    if content is not None:
        attach_IO(msg, content, ContentFileName)

    # print(addr_from, password)


    # ======== Этот блок настраивается для каждого почтового провайдера отдельно ===============================================
    server = smtplib.SMTP_SSL('smtp.yandex.ru', 465)  # Создаем объект SMTP
    # if USE_TLS:
    # server.starttls()                                      # Начинаем шифрованный обмен по TLS
    server.set_debuglevel(False)  # Включаем режим отладки, если не нужен - можно закомментировать
    server.login(addr_from, password)  # Получаем доступ
    server.send_message(msg)  # Отправляем сообщение
    server.quit()  # Выходим
    # ==========================================================================================================================


def attach_IO(msg, content, filename):
    ctype = 'application/octet-stream'  # Будем использовать общий тип
    maintype, subtype = ctype.split('/', 1)
    file = MIMEBase(maintype, subtype)  # Используем общий MIME-тип
    file.set_payload(content.read())  # Добавляем содержимое общего типа (полезную нагрузку)
    content.close()
    encoders.encode_base64(file)  # Содержимое должно кодироваться как Base64
    file.add_header('Content-Disposition', 'attachment', filename=filename)  # Добавляем заголовки
    msg.attach(file)


def process_attachement(msg, files):  # Функция по обработке списка, добавляемых к сообщению файлов
    for f in files:
        if os.path.isfile(f):  # Если файл существует
            attach_file(msg, f)  # Добавляем файл к сообщению
        elif os.path.exists(f):  # Если путь не файл и существует, значит - папка
            dir = os.listdir(f)  # Получаем список файлов в папке
            for file in dir:  # Перебираем все файлы и...
                attach_file(msg, f + "/" + file)  # ...добавляем каждый файл к сообщению


def attach_file(msg, filepath):  # Функция по добавлению конкретного файла к сообщению
    filename = os.path.basename(filepath)  # Получаем только имя файла
    ctype, encoding = mimetypes.guess_type(filepath)  # Определяем тип файла на основе его расширения
    if ctype is None or encoding is not None:  # Если тип файла не определяется
        ctype = 'application/octet-stream'  # Будем использовать общий тип
    maintype, subtype = ctype.split('/', 1)  # Получаем тип и подтип
    if maintype == 'text':  # Если текстовый файл
        with open(filepath) as fp:  # Открываем файл для чтения
            file = MIMEText(fp.read(), _subtype=subtype)  # Используем тип MIMEText
            fp.close()  # После использования файл обязательно нужно закрыть
    elif maintype == 'image':  # Если изображение
        with open(filepath, 'rb') as fp:
            file = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
    elif maintype == 'audio':  # Если аудио
        with open(filepath, 'rb') as fp:
            file = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
    else:  # Неизвестный тип файла
        with open(filepath, 'rb') as fp:
            file = MIMEBase(maintype, subtype)  # Используем общий MIME-тип
            file.set_payload(fp.read())  # Добавляем содержимое общего типа (полезную нагрузку)
            fp.close()
            encoders.encode_base64(file)  # Содержимое должно кодироваться как Base64
    file.add_header('Content-Disposition', 'attachment', filename=filename)  # Добавляем заголовки
    msg.attach(file)  # Присоединяем файл к сообщению


def db(conn):
    config = configparser.ConfigParser()
    config.sections()
    config_path = os.path.abspath(os.path.dirname(__file__)) + '/data_juggler.ini'
    config.read(config_path)

    driver = config[conn.scheme]['driver']
    port = conn.port
    database = conn.path.split("/")[1]  # TODO Переделать с учетом инстанса
    server = conn.hostname
    if port is None:
        port = config[conn.scheme]['default_port']
    conn_str = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=' + str(port) + ';DATABASE=' + database + ';'

    if conn.username is None:
        conn_str = conn_str + 'Trusted_Connection=yes;'
    else:
        conn_str = conn_str + 'UID=' + conn.username + ';PWD=' + conn.password + ';'

    # print(conn_str)

    return pyodbc.connect(
        conn_str)  # pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)


def format_value(value):
    if value is None:
        return ''
    elif isinstance(value, str):
        return value
    elif isinstance(value, int):
        return value
    elif isinstance(value, datetime.datetime):
        return value.strftime("%d.%m.%Y %H:%M:%S")
    elif isinstance(value, datetime.date):
        return value.strftime("%d.%m.%Y")
    elif isinstance(value, decimal.Decimal):
        return float(value)
    return str(value)


def next_all(cur):
    """

    :param cur: executet cur
    :return: list
    """
    l = []
    if cur.nextset():
        rows = cur.fetchall()
        if rows:
            l = [dict((cur.description[i][0], format_value(value)) for i, value in enumerate(row)) for row in rows]
    return l


def next_one(cur):
    """
    :param cur: executet cur
    :return: dict
    """
    d = {}
    if cur.nextset():
        row = cur.fetchone()
        if row:
            d = dict((cur.description[i][0], format_value(value)) for i, value in enumerate(row))
    return d


def fill_dict(d, cur):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k].update(next_one(cur))
            fill_dict(v, cur)
        if isinstance(v, list):
            d[k] = (next_all(cur))


def http_post(url, body=None, gzip=True):
    req = request.Request(url)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    if isinstance(body, str):
        jsondataasbytes = body.encode('utf-8')  # needs to be bytes
    elif isinstance(body, bytes):
        jsondataasbytes = body
    else:
        raise Exception("body unknown")
        return False

    # req.add_header('Content-Length', len(jsondataasbytes))
    if gzip:
        body_post = gzip_lib.compress(jsondataasbytes)
        req.add_header('Content-Encoding', 'gzip')
    else:
        body_post = jsondataasbytes

    req.add_header('Content-Length', len(body_post))
    response = request.urlopen(req, body_post)

    # print(response.getcode())
    htmlIO = response.read()  # .decode('utf8').encode(codepage_out)
    # print(response.getcode(),html)
    # if response.getcode() == 200:
    #     # print(html)
    #     return True
    # else:
    #     print(response.getcode())
    return response.getcode(), htmlIO


def iri_to_uri(iri):
    parts = urlsplit(iri)
    uri = urlunsplit((
        parts.scheme,
        parts.netloc.encode('idna').decode('ascii'),
        quote(parts.path),
        quote(parts.query, '='),
        quote(parts.fragment),
    ))
    return uri


# def to_sftp():
#     with pysftp.Connection('hostname', username='me', password='secret') as sftp:
#         with sftp.cd('public'):  # temporarily chdir to public
#             sftp.put('/my/local/filename')  # upload file to public/ on remote
#             sftp.get('remote_file')  # get a remote file

def to_csv(dataset):
    return to_csvIO(dataset).getvalue()


def to_csvIO(dataset, delimiter='\t'):
    iostring = io.StringIO()
    w = csv.DictWriter(iostring, dataset[0].keys(), delimiter=delimiter)
    w.writeheader()
    for row in dataset:
        w.writerow(row)
    return iostring


def to_spryreport(template_filename, data):
    return spryreport.spryreport(template_filename, data)


def to_zip(file_list):
    return to_zipIO(file_list).getvalue()


def to_zipIO(file_list):
    """
    :param file_list: list of [file_name, file_content]
    :return: io.BytesIO
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, file_content in file_list:
            # zip_file.writestr(file_name, file_content.getvalue().encode('cp1251'))
            zip_file.writestr(file_name, file_content)
    zip_buffer.seek(0)
    return zip_buffer


def to_file(file_name, file_content):
    # print(file_name)
    with open(file_name, 'wb') as f:
        f.write(file_content)
        f.close()
    print(file_name, 'OK')
    return


def from_file(file_name):
    # print(file_name)
    with open(file_name, 'rb') as f:
        fileIO = f.read()
        f.close()
    return fileIO


def from_xml(value):
    return xmltodict.parse(value)


def from_json(value):
    return ast.literal_eval(value)


def to_jsonIO(data):
    return to_json(data).encode()


def to_json(data):
    # encoder.FLOAT_REPR = lambda o: format(o, '.2f')
    return json.dumps(data, ensure_ascii=False, default=str)


def main(sql_u, baseurl, params, url_login, url_password):
    # reload(sys)
    # sys.setdefaultencoding('utf-8')
    # или

    # sys.setdefaultencoding(locale.getpreferredencoding())

    d_params = {}
    if params:
        for v in params:
            d_params.update(dict(parse_qsl(v)))
            # print (v, parse_qsl(v))

    if sql_u:
        d = query_db(sql_u)
        json_output = json.dumps(d, ensure_ascii=False)

        if baseurl:  # --url_base "http://127.0.0.1:8000/spryreport/report/"
            if url_login:
                install_opener(baseurl, url_login, url_password)

            url = baseurl + '?' + urlencode(d_params)
            # print (json_output)
            http_post(url, json_output)
        else:
            # print('Привет')  # Вернем JSON
            # b = bytes(json_output, 'utf-8')
            print(json_output)  # Вернем JSON Проблема с кодировкой. MSSQL не может подобрать без потери кодировки
    return

    # book = notin.report_by_template("media/Отчет по изменению СМ.xlsx", d)
    # book.save("media/Отчет по изменению СМ 20181102.xlsx")


def createParser():
    parser = argparse.ArgumentParser()
    # parser.add_argument('--json', required=True)
    # parser.add_argument('--sql', nargs='+', default=None)

    parser.add_argument('--source', default=None)
    parser.add_argument('--destination', default=None)
    parser.add_argument('--source_format', default=None)
    parser.add_argument('--destination_format', default=None)

    parser.add_argument('--sql_u', default=None)
    parser.add_argument('--url_base', default=None)
    parser.add_argument('--url_params', nargs='+', default=[])
    parser.add_argument('--url_login', default=None)
    parser.add_argument('--url_password', default=None)
    return parser


def new_config():
    config = configparser.ConfigParser()
    config['sqlserver'] = {'port': '1433', 'driver': '{SQL Server Native Client 10.0}'}
    with open('data_juggler.ini', 'w') as configfile:
        config.write(configfile)


def config_read():
    config = configparser.ConfigParser()
    config.sections()
    config_path = os.path.abspath(os.path.dirname(__file__)) + '/data_juggler.ini'
    config.read(config_path)
    print(config['connection.fusi']['connection_type'])


# def query_db(query, args=()):
#     cur = db().cursor()
#     cur.execute(query, args)
#
#     row = cur.fetchone() #Структра json
#     result = ast.literal_eval((row[0])) #Первая колонка в первой строке
#
#     fill_dict(result, cur)
#     cur.connection.close()
#     return result


def get_named_list(cur):
    rows = cur.fetchall()
    if rows:
        return [dict((cur.description[i][0], format_value(value)) for i, value in enumerate(row)) for row in rows]


def query_db(o, data, commit=False):
    con = db(o)
    cur = con.cursor()
    for k, query in parse_qsl(o.query):
        cur.execute(dequote(query))
        if data.get(k):
            data[k].append(get_named_list(cur))
        else:
            data[k] = [get_named_list(cur)]
        while cur.nextset():
            data[k].append(get_named_list(cur))
    if commit:
        cur.commit()
    # cur.commit()
    cur.close()
    return data


# def exec_db(o):
#     cur = db(o).cursor()
#     for k, query in parse_qsl(o.query):
#         cur.execute(dequote(query))
#         cur.fetchall()

def load_data(source, commit=False):
    o = urlparse(source, False)
    data = {}
    if o.scheme in ['sqlserver']:
        data = query_db(o=o, data=data, commit=commit)
    return data


def dequote(s):
    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]
    return s


# def exec_data(source):
#     o = urlparse(source, False)
#     # data = {}
#     if o.scheme in ['sqlserver']:
#         exec_db(o)
#     return True


def save_data(destination, destination_data):
    o = urlparse(destination, False)
    if o.scheme in ['file']:
        # print(o)
        if o.netloc in ['localhost', '']:
            file_path = o.path[1:]
        else:
            file_path = '//' + o.netloc + o.path
        to_file(file_path, destination_data)
    return True


if __name__ == '__main__':
    print ('data_juggler')


