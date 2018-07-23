import logging
from subprocess import call

import luigi
import sciluigi
from sciluigi import TargetInfo

import src.preprocess

class Europarl(sciluigi.ExternalTask):
    target = sciluigi.Parameter()
    source = sciluigi.Parameter()

    def out_europarl(self):
        return [ TargetInfo(self, self.target),
                TargetInfo(self, self.source) ]


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
        self.ex('wc -l raw/europarl/*', shell=True)
        logging.info('Line count of preprocessed:')
        self.ex('wc -l data/europarl/*', shell=True)


class Split(sciluigi.Task):
    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    in_preproc = None 

    def out_splits(self):
        return [ TargetInfo(self, 'data/splits/%s' % ('%d' % i).zfill(self.digits)) \
                for i in range(0, 201) ]

    def run(self):
        self.ex('mkdir -p data/splits/')
        self.ex('split -d -l %d -a %d %s data/splits/' %  \
                (self.splits, self.digits, self.in_preproc[1].path)) 


class ParseWithLogon(sciluigi.Task):
    in_splits = None

    def out_parse(self):
        pass


class RunFRtoEN(sciluigi.WorkflowTask):
    def workflow(self):
        europarl = self.new_task('Europarl', Europarl,
                source='raw/europarl/europarl-v7.fr-en.fr',
                target='raw/europarl/europarl-v7.fr-en.en')

        preprocess = self.new_task('Preprocess Europarl', PreprocessEuroparl)
        preprocess.in_europarl = europarl.out_europarl()

        split = self.new_task('Split Europarl', Split,
                splits=10000)
        split.in_preproc = europarl.out_europarl()

        return split


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    luigi.run(main_task_cls=RunFRtoEN, local_scheduler=True)
