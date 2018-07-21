import logging
from subprocess import call

import luigi
import sciluigi
from sciluigi import TargetInfo

import src.preprocess

class Europarl(sciluigi.ExternalTask):
    def out_europarl(self):
        return [ TargetInfo(self, 'raw/europarl/europarl-v7.fr-en.fr'),
                TargetInfo(self, 'raw/europarl/europarl-v7.fr-en.en') ]


class PreprocessEuroparl(sciluigi.Task):
    in_europarl = None

    def out_preproc(self):
        return [ TargetInfo(self, 'data/europarl/europarl-v7.fr-en.fr'),
                TargetInfo(self, 'data/europarl/europarl-v7.fr-en.en') ]

    def run(self):
        call('mkdir data/europarl/', shell=True)
        xml, blank = src.preprocess.preprocess_europarl(self.in_europarl[0].path,
                self.in_europarl[1].path,
                self.out_preproc()[0].path,
                self.out_preproc()[1].path)
        logging.info('Removed %d lines with xml' % xml)
        logging.info('Removed %d blank lines' % blank)

        call('wc -l raw/europarl/*', shell=True)
        call('wc -l data/europarl/*', shell=True)
        

class Run(sciluigi.WorkflowTask):
    def workflow(self):
        europarl = self.new_task('Europarl', Europarl)
        preprocess = self.new_task('Preprocess Europarl', PreprocessEuroparl)
        preprocess.in_europarl = europarl.out_europarl()

        return preprocess


if __name__ == '__main__':
    luigi.run(main_task_cls=Run, local_scheduler=True)
