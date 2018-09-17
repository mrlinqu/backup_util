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
                return False
        return True

    def cmd_backup(self):
        backupTimestamp = datetime.now().strftime('%Y%m%dT%H%M%S%z')
        for dirname, dirnames, filenames in os.walk(self.config['src']):
            for filename in filenames:
                srcname = os.path.join(dirname, filename)
                relname = srcname.replace(self.config['src'], '', 1)

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

                if (not len(self.cur.fetchall())):
                    if ('arc_mode' in self.config and self.config['arc_mode'] == '1'):
                        mirname = os.path.normpath(self.config['dest'] + '/' + relname + self.archExt)
                    else:
                        mirname = os.path.normpath(self.config['dest'] + '/mirror/' + relname + self.archExt)

                    if (os.path.isfile(mirname)):
                        if ('arc_mode' in self.config and self.config['arc_mode'] == '1'):
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
                        f_out = bz2.BZ2File(mirname, 'wb', compresslevel=9)
                    elif ('compression' in self.config and self.config['compression'] == 'gz'):
                        f_out = gzip.GzipFile(mirname, 'wb', compresslevel=9)
                    else:
                        f_out = open(mirname, 'wb')

                    shutil.copyfileobj(f_in, f_out)

                    f_in.close()
                    f_out.close()

                    self.cur.execute('SELECT * FROM files WHERE fullname=?', (relname,))

                    if (len(self.cur.fetchall())):
                        sql = 'UPDATE files SET date=?, sum=? WHERE fullname=?'
                        sql_dt = (file_date, file_sum, relname,)
                    else:
                        sql = 'INSERT INTO files VALUES (?,?,?)'
                        sql_dt = (relname, file_date, file_sum,)

                    self.cur.execute(sql, sql_dt)
                    self.conn.commit()

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
        if ('arc_mode' in self.config and self.config['arc_mode'] == '1'):
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
