import os
import re
import math
from collections import defaultdict

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


def create_dict(export_dir):
    # read in db
    db = {}
    for fn in sorted(os.listdir(export_dir)):
        if fn.startswith('.'):
            continue

        print('Reading in %s \r' % fn, end='')
        h = open(os.path.join(export_dir, fn))

        for line in h:
            line = line.strip().split('\t')
            db[line[0]] = left_to_right_traversal(line[1]) if line[1] != 'NA' else 'NA'
            
    print('Read in %d parses.' % len(db))

    def query(s, db):
        return db.get(s, 'error')
    return lambda s: query(s, db)


def main(source, reference, translation, ref_export, trans_export, output='data/analysis/data.tsv'):
    unigram = MLEUnigramModel(source)
    source, reference = open(source), open(reference)
    translation = (i.split(' ||| ') for i in open(translation))
    ref_query, trans_query = create_dict(ref_export), create_dict(trans_export)

    output = open(output, 'wt')

    for source, ref, trans in zip(source, reference, translation):
        row = []

        source, ref, trans[1] = source.strip(), ref.strip(), trans[1].strip()

        row.append(source)
        row.append(str(unigram.score(source)))
        row.append(ref)
        row.append(ref_query(ref))
        row.append(trans[1])
        row.append(trans_query(trans[1]))
        row.append(trans[2])

        output.write('\t'.join(row) + '\n')


if __name__ == '__main__':
    main(
        'data/translate/splits/analysis.source',
        'data/translate/splits/analysis.target',
        'data/translate/output/argmax.out',
        'data/export/pre-parse/',
        'data/export/post-parse/'
    )
