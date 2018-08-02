import os
import collections
import sys

db = dict()

def build(fn):
    global db

    print('Reading in %s \r' % fn, end='')
    for line in open(fn, 'rt'):
        line = line.strip().split('\t')
        db[line[0]] = line[2]


def query(sent):
    return db.get(sent, 'NA')


def decide(fr, en, _fr, _en):
    fr = open(fr, 'rt')
    en = open(en, 'rt')

    _fr = open(_fr, 'wt')
    _en = open(_en, 'wt')

    for eng, fren in zip(en, fr):
        # should never happen
        if fr == '\n' or en == '\n':
            raise Exception('Double check Europarl preprocessing.')
        
        preproc = query(eng.strip())

        if preproc != 'NA':
            _fr.write('%s\n' % fren.strip())
            _en.write('%s\n' % preproc.strip())
