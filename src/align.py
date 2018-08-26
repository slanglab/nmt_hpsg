import os
import re
import math
import itertools
from collections import defaultdict

from unknowns import replace_unk

class MLEUnigramModel:
    def __init__(self, train_file):
        self.d = defaultdict(int)
        for line in open(train_file):
            for word in line.split():
                self.d[word] += 1

        self.ln_sum = math.log(sum([ i[1] for i in self.d.items() ]))

    def score(self, sent):
        return -(-len(sent) * self.ln_sum + \
                sum([ math.log(self.d.get(word, 1)) for word in sent ]))

    def count(self, word):
        return self.d.get(word, 0)


def left_to_right_traversal(s):
    traversal = s[:s.index(' ')] + ' '
    for match in re.findall(r'(\(\d+ [A-z-_]+)|(\(".+?")|(\))', s):
        if match[0] != '':
            traversal += '(%s ' % match[0].split()[1]
        elif match[1] != '':
            traversal += match[1] + ' '
        else:
            traversal += ')'

    return traversal

def create_dict(export_dir, export_field):
    # read in db
    db = {}
    for fn in sorted(os.listdir(export_dir)):
        if fn.startswith('.'):
            continue

        print('Reading in %s \r' % fn, end='')
        h = open(os.path.join(export_dir, fn))

        for line in h:
            line = line.strip('\n').split('\t')
            line[1] = left_to_right_traversal(line[1]) if line[1] != 'NA' else 'NA'
            db[line[export_field]] = line

    print('Read in %d parses.' % len(db))

    def query(s, db):
        return db.get(s, ['error'] * 4)
    return lambda s: query(s, db)


def main(source, reference, translation, ref_export, trans_export, output='data/analysis/data.tsv'):
    unigram = MLEUnigramModel('data/translate/splits/train.source')
    source = open(source)
    reference = open(reference)
    reference_no_tok = iter(open('data/pre/shuffled/target').readlines()[1400754:])   # this is a hack...
    translation = (i.split(' ||| ') for i in open(translation))
    ref_query, trans_query = create_dict(ref_export, 2), create_dict(trans_export, 0)

    output = open(output, 'wt')

    i = 0
    for src, ref, trans in zip(source, reference, translation):
        row = []

        src, ref, trans[1], trans[2] = \
                src.strip(), ref.strip(), trans[1].strip(), trans[2].strip()

        rq = ref_query(next(reference_no_tok).strip())
        tq = trans_query(replace_unk(trans[1]))

        row.append(src)
        row.append(ref)
        row.append(trans[1])
        #row.append(ref_query(ref))
        row.append(rq[1])
        row.append(tq[1])
        row.append(tq[3])
        row.append(str(unigram.score(src)))
        row.append(trans[2])

        if tq[1] == 'error' or rq[1] == 'error':
            i += 1

        output.write('\t'.join(row) + '\n')

    print('We had %d errors in total.' % i)


if __name__ == '__main__':
    main(
        'data/translate/splits/analysis.source',
        'data/translate/splits/analysis.target',
        'data/translate/output/argmax.out',
        'data/export/pre-parse/',
        'data/export/post-parse/'
    )
