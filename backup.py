import sqlite3
import os
from datetime import datetime
import gzip
import bz2
import shutil
from fnmatch import fnmatch

class Backup:

    archFolderPostfix = '__archdir__'

    def __init__(self, config):
        self.config = config
        self.archExt = '.'+self.config['compression'] if 'compression' in self.config else ''
        self.init_db()

        self.copyCountsPaths = []

        #for k in config['copyCounts']:
        #    self.copyCountsPaths.append(k)


    def file_md5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def make_path(fname):
        dirname = os.path.dirname(fname)
        if not os.path.exists(dirname):
            os.makedirs(dirname, 0o777, True)


    def init_db(self):
        self.conn = sqlite3.connect(os.path.join(self.config['dest'], '._archdb.sqlite'))
        self.cur = self.conn.cursor()
        self.cur.execute('create table if not exists files (fullname text, date integer, sum text)')
        self.conn.commit()

    def dispatch(self, command, params):
        method_name = 'cmd_' + str(command)
        method = getattr(self, method_name)
        return method(*params)

    def in_exlude(self, fname):
        for exlude in self.config['exclude']:
            if (fnmatch(fname, exlude)):
                return True
        return False

    def cmd_backup(self):
        backupTimestamp = datetime.now().strftime('%Y%m%dT%H%M%S%z')
        for dirname, dirnames, filenames in os.walk(self.config['src']):
            for filename in filenames:
                srcname = os.path.join(dirname, filename)
                relname = srcname.replace(self.config['src'], '', 1)

                ########################################################################
                # в списке исключений - игнорируем
                if (self.in_exlude(relname)):
                    continue

                sql = 'SELECT * FROM files WHERE fullname=? AND '
                sqldt = (relname,)

                file_date = os.path.getmtime(srcname)
                file_sum = ''

                if ('compare' in self.config and self.config['compare']=='sum'):
                    sql += 'sum = ?'
                    file_sum = file_md5(srcname)
                    sqldt += (file_sum,)
                else:
                    sql += 'date = ?'
                    sqldt += (str(file_date),)

                self.cur.execute(sql, sqldt)

                ########################################################################
                # совпадений не найдено - копируем
                if (not len(self.cur.fetchall())):
                    if (self.config['arcMode'] == '1'):
                        mirname = os.path.normpath(self.config['dest'] + '/' + relname + self.archExt)
                    else:
                        mirname = os.path.normpath(self.config['dest'] + '/mirror/' + relname + self.archExt)

                    ########################################################################
                    # файл уже есть в зеркале - перенести в архив
                    archname = ''
                    if (os.path.isfile(mirname)):
                        if (self.config['arcMode'] == '1'):
                            #get extension?
                            archname = os.path.normpath(mirname + Backup.archFolderPostfix + '/' + backupTimestamp + self.archExt)
                        else:
                            archname = os.path.normpath(self.config['dest'] + '/arch/' + backupTimestamp + '/' + relname + self.archExt)

                        Backup.make_path(archname)
                        os.rename(mirname, archname)


                    Backup.make_path(srcname)
                    Backup.make_path(mirname)

                    f_in = open(srcname, 'rb')

                    if ('compression' in self.config and self.config['compression'] == 'bz2'):
                        f_out = bz2.BZ2File(mirname, 'wb', compresslevel = self.config['compressionLevel'])
                    elif ('compression' in self.config and self.config['compression'] == 'gz'):
                        f_out = gzip.GzipFile(mirname, 'wb', compresslevel = self.config['compressionLevel'])
                    else:
                        f_out = open(mirname, 'wb')

                    shutil.copyfileobj(f_in, f_out)

                    f_in.close()
                    f_out.close()

                    ########################################################################
                    # создаем/обновляем запись в базе

                    self.cur.execute('SELECT * FROM files WHERE fullname=?', (relname,))

                    if (len(self.cur.fetchall())):
                        sql = 'UPDATE files SET date=?, sum=? WHERE fullname=?'
                        sql_dt = (file_date, file_sum, relname,)
                    else:
                        sql = 'INSERT INTO files VALUES (?,?,?)'
                        sql_dt = (relname, file_date, file_sum,)

                    self.cur.execute(sql, sql_dt)
                    self.conn.commit()

                    ########################################################################
                    # удаление старых копий

                    if (archname != ''):
                        maxCopyCount = int(self.config['maxCopyCount'])
                        for k in self.config['maxCopyCounts']:
                            if (fnmatch(relname, k)):
                                maxCopyCount = int(self.config['maxCopyCounts'][k])
                                break

                        if maxCopyCount > 0:
                            fileList = []
                            if (self.config['arcMode'] == '1'):
                                dir_name = os.path.normpath(mirname + Backup.archFolderPostfix)
                                fileList = [os.path.normpath(dir_name + '/' + f) for f in os.listdir(dir_name)]
                            else:
                                dir_name = os.path.normpath(self.config['dest'] + '/arch/')
                                mask = os.path.normpath(self.config['dest'] + '/arch/*/' + relname + self.archExt)
                                for root, dirs, files in os.walk(dir_name):
                                    fileList += [os.path.join(root, f) for f in files if fnmatch(os.path.join(root, f), mask)]

                            fileList.sort()

                            while (len(fileList) > maxCopyCount):
                                f = fileList.pop(0)
                                os.remove(f)

    def decompress(self, src, dst):
        (directory, filename) = os.path.split(src)
        (fname, ext) = os.path.splitext(filename)

        destination = os.path.join(dst, fname)

        if (self.config['compression'] == 'bz2'):
             f_in = bz2.BZ2File(src, 'rb')
        elif (self.config['compression'] == 'gz'):
            f_in = gzip.GzipFile(src, 'rb')
        else:
            f_in = open(src, 'rb')
            destination = os.path.join(dst, fname + ext)

        Backup.make_path(destination)
        f_out = open(destination, 'wb')
        shutil.copyfileobj(f_in, f_out)

        f_in.close()
        f_out.close()

    def cmd_restore(self, src, dst):
        if (self.config['arcMode'] == '1'):
            mirname = os.path.normpath(self.config['dest'] + '/' + src)
        else:
            mirname = os.path.normpath(self.config['dest'] + '/mirror/' + src)

        fname = os.path.normpath(mirname + self.archExt)
        if (os.path.isfile(fname)):
            self.decompress(fname, dst)
        elif (not os.path.exists(mirname)):
            print('Path not found!')
            return
        else:
            for dirname, dirnames, filenames in os.walk(mirname):
                for filename in filenames:
                    srcname = os.path.join(dirname, filename)
                    dirnm = dirname.replace(mirname, '', 1)
                    destname = os.path.normpath(dst + '/' + dirnm)
                    self.decompress(srcname, destname)


    def cmd_history(path):
        pass

    def cmd_ls(self, path):
        print(path)
        pass
