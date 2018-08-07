import os
import logging
from subprocess import call

import luigi
import sciluigi
from sciluigi import TargetInfo

import src.preprocess, src.filter, src.shuffle

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


class RunFRtoEN(sciluigi.WorkflowTask):
    def workflow(self):
        europarl = self.new_task('Europarl', Europarl,
                source='raw/europarl/europarl-v7.fr-en.fr',
                target='raw/europarl/europarl-v7.fr-en.en')

        preprocess = self.new_task('Preprocess Europarl', PreprocessEuroparl)
        preprocess.in_europarl = europarl.out_europarl()

        # parse and filter
        splits = 300
        split = self.new_task('Split Europarl', Split,
                splits=splits)
        split.in_preproc = preprocess.out_preproc()

        pre_parse = self.new_task('Parse input with PET', ParseWithPET,
                splits=splits)
        pre_parse.in_splits = split.out_splits()

        export = self.new_task('Export parses', ExportFromTSDB,
                splits=splits)
        export.in_parse = pre_parse.out_parse()

        flter = self.new_task('Filter parallel data', FilterParallelData)
        flter.in_preproc = preprocess.out_preproc()
        flter.in_export = export.out_export()

        shuffle = self.new_task('Shuffle parallel data', ShuffleParallelData)
        shuffle.in_parallel = flter.out_filter()

        return shuffle


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    luigi.run(main_task_cls=RunFRtoEN, local_scheduler=True)
