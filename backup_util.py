#!/usr/bin/env python3

import argparse
import configparser
from backup import Backup

parser = argparse.ArgumentParser(description='Backup util.')
#parser.add_argument('integers', metavar='N', type=int, nargs='+',
#                   help='an integer for the accumulator')
#parser.add_argument('--sum', dest='accumulate', action='store_const',
#                   const=sum, default=max,
#                   help='sum the integers (default: find the max)')

parser.add_argument('command', help="Command", choices=['backup', 'restore', 'history', 'ls'])
parser.add_argument('-c', '--config', default='backup_util.conf', help="Config file (default: backup_util.conf)")
parser.add_argument('param', nargs='*', help="Command param")


args = parser.parse_args()
config = configparser.ConfigParser()
config.read_file(open(args.config))

confMain = config['main']

if not 'src' in confMain:
    print('Config file: src param is not configured')
    exit()

if not 'dest' in confMain:
    print('Config file: dest param is not configured')
    exit()

conf = {
	'src': confMain.get('src'),
	'dest': confMain.get('dest'),
	'arcMode': confMain.get('arcMode', 1),
	'compression': confMain.get('compression', 'gz'),
	'compressionLevel': confMain.get('compressionLevel', 9),
	'maxCopyCount': confMain.get('maxCopyCount', 0),
	'exclude': confMain.get('exclude', []),
	'compare': confMain.get('compare', 'date'),
	'maxCopyCounts': config['maxCopyCounts'],
}
#conf['src'] = confMain.get('src')
#conf['dest'] = confMain.get('dest')
#conf['arcMode'] = confMain.get('arcMode', 1)
#conf['compression'] = confMain.get('compression', 'gz')
#conf['compressionLevel'] = confMain.get('compressionLevel', 9)
#conf['copyCount'] = confMain.get('copyCount', 0)
#conf['exclude'] = confMain.get('exclude', [,])
#conf['compare'] = confMain.get('compare', 'date')
#conf['copyCounts'] = config['copyCounts']


b = Backup(conf)
b.dispatch(args.command, args.param)
