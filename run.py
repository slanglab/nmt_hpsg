import os
import logging
from subprocess import call

import luigi
import sciluigi
from sciluigi import TargetInfo

from parse import Parse
from translate import TrainAndTranslate
import src.preprocess, src.filter, src.shuffle, src.align

class Europarl(sciluigi.ExternalTask):
    target = sciluigi.Parameter()
    source = sciluigi.Parameter()

    def out_europarl(self):
        return [ TargetInfo(self, self.source),
                TargetInfo(self, self.target) ]


class PreprocessEuroparl(sciluigi.Task):
    in_europarl = None

    def out_preproc(self):
        return [ TargetInfo(self, 'data/pre/preprocess/source'),
                TargetInfo(self, 'data/pre/preprocess/target') ]

    def run(self):
        self.ex('mkdir -p data/pre/preprocess/')
        xml, blank = src.preprocess.preprocess_europarl(self.in_europarl[0].path,
                self.in_europarl[1].path,
                self.out_preproc()[0].path,
                self.out_preproc()[1].path)
        logging.info('Removed %d lines with xml' % xml)
        logging.info('Removed %d blank lines' % blank)

        logging.info('Line count of inputs:')
        call('wc -l raw/europarl/*', shell=True)
        logging.info('Line count of preprocessed:')
        call('wc -l data/pre/preprocess/*', shell=True)


class FilterParallelData(sciluigi.Task):
    in_preproc = None
    in_export = None

    def out_filter(self):
        return [ TargetInfo(self, 'data/pre/decided/source'),
                TargetInfo(self, 'data/pre/decided/target') ]

    def run(self):
        self.ex('mkdir -p data/pre/decided/')
        [ src.filter.build(export.path) for export in self.in_export ]
        src.filter.decide(self.in_preproc[0].path,
                self.in_preproc[1].path,
                self.out_filtered()[0].path,
                self.out_filtered()[1].path)

        logging.info('Line count of data:')
        call('wc -l data/pre/decided/*', shell=True)


class ShuffleParallelData(sciluigi.Task):
    in_parallel = None

    def out_shuffle(self):
        return [ TargetInfo(self, 'data/pre/shuffled/source'),
                TargetInfo(self, 'data/pre/shuffled/target') ]

    def run(self):
        self.ex('mkdir -p data/pre/shuffled/')
        src.shuffle.parallel_shuffle(self.in_parallel[0].path,
                self.in_parallel[1].path,
                self.out_shuffle()[0].path,
                self.out_shuffle()[1].path)


class Pick(sciluigi.Task):
    in_translations = None

    def out_pick(self):
        return [ TargetInfo(self, 'data/translate/output/argmax.out'),
                TargetInfo(self, 'data/translate/output/argmax.txt') ]

    def run(self):
        self.ex("awk 'NR == 1 || NR %% 5 == 1' %s > %s" % 
                (self.in_translations.path, self.out_pick()[0].path))
        self.ex('cat data/translate/output/argmax.out | sed "s/ ||| /|/g" | cut -f2 -d "|" > %s' % self.out_pick()[1].path)


class ReplaceUnknowns(sciluigi.Task):
    in_pick = None

    def out_replace(self):
        return TargetInfo(self, 'data/post/replaced/to_parse')

    def run(self):
        self.ex('mkdir -p data/post/')
        self.ex('mkdir -p data/post/replaced')
        self.ex('cat %s | python src/unknowns.py > %s' %
                (self.in_pick[1].path, self.out_replace().path))


class AlignForAnalysis(sciluigi.Task):
    in_data = None

    def out_align(self):
        return TargetInfo(self, 'data/analysis/data.tsv')

    def run(self):
        src.align.main(
            'data/translate/splits/analysis.source',
            'data/translate/splits/analysis.target',
            'data/translate/output/argmax.out',
            'data/export/pre-parse/',
            'data/export/post-parse/'
        )

class RunFRtoEN(sciluigi.WorkflowTask):
    def workflow(self):
        europarl = self.new_task('Europarl', Europarl,
                source='raw/europarl/europarl-v7.fr-en.fr',
                target='raw/europarl/europarl-v7.fr-en.en')

        # pre
        preprocess = self.new_task('Preprocess Europarl', PreprocessEuroparl)
        preprocess.in_europarl = europarl.out_europarl()

        pre_parse = self.new_task('Parse training+test data', Parse,
                run='pre-parse',
                splits=300)
        pre_parse.in_text = preprocess.out_preproc()

        flter = self.new_task('Filter parallel data', FilterParallelData)
        flter.in_preproc = preprocess.out_preproc()
        flter.in_export = pre_parse.out_export()

        shuffle = self.new_task('Shuffle parallel data', ShuffleParallelData)
        shuffle.in_parallel = flter.out_filter()

        # nematus
        train = self.new_task('Train and translate', TrainAndTranslate)
        train.in_data = shuffle.out_shuffle()

        pick = self.new_task('Pick out best translations', Pick)
        pick.in_translations = train.out_translations()

        # post
        replace = self.new_task('Replace unknowns', ReplaceUnknowns)
        replace.in_pick = pick.out_pick()

        post_parse = self.new_task('Parse translations', Parse,
                nrun='post-parse',
                splits=50)
        post_parse.in_text = replace.out_replace()

        # prepare analysis
        align = self.new_task('Align results for analysis', AlignForAnalysis)
        align.in_data = post_parse.out_export()

        return align


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    luigi.run(main_task_cls=RunFRtoEN, local_scheduler=True)
