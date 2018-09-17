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

if not args.config:
    print('aaaa')

config = configparser.ConfigParser()
config.read_file(open(args.config))

conf = config['main']

if not 'src' in conf:
    print('Config file: src param is not configured')
    exit()

if not 'dest' in conf:
    print('Config file: dest param is not configured')
    exit()

#if not 'compression' in conf:
#    conf['compression'] = 'gz' #bz2 xz
if not 'compression_level' in conf:
    conf['compression_level'] = '9'

b = Backup(conf)
b.dispatch(args.command, args.param)
