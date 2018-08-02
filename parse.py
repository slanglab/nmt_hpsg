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


# TODO filter english sentences to parse based on
# sentences that are already parsed...
class FilterParsed(sciluigi.Task):
    pass


class Split(sciluigi.Task):
    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    in_preproc = None 

    def out_splits(self):
        return [ TargetInfo(self, 'data/pre/splits/%s' % ('%d' % i).zfill(self.digits)) \
                for i in range(0, self.splits) ]    #need to figure out how to get 201 programmatically

    def run(self):
        self.ex('mkdir -p data/pre/splits/')
        self.ex('split -d -l $((`wc -l < %s`/%d)) -a %d %s data/pre/splits/' %  \
                (self.in_preproc[1].path, self.splits, self.digits, self.in_preproc[1].path)) 


# PET parsing should be refactored into its own
# workflow for pre and post results.
class ParseWithPET(sciluigi.Task):
    tsdb = luigi.Parameter(default='./logon/lingo/lkb/src/tsdb/home/erg/1214/')
    compute = luigi.Parameter(default='blake')

    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    in_splits = None

    def out_parse(self):
        return [ TargetInfo(self, os.path.join(self.tsdb + str(i).zfill(self.digits))) \
                for i in range(0, self.splits) ]

    def run(self):
        logging.info('Send jobs? [Y|n]')
        if input() == 'n':
            exit()

        #clear tsdb results
        self.ex('rm -r %s || true' % os.path.join(self.tsdb, '*'))
        self.ex('rm log/slurm/* || true')

        if self.compute == 'blake':
            # The following code is specific to blake2.cs.umass.edu
            # as there are 24 and 48 core CPU nodes, and each logon
            # parse script can only be run on one node at a time
            split_idx = int(self.splits * 0.66)

            self.ex(('sbatch --array=0-%d --ntasks=24 --mem=48G --export=digits=%d,ntasks=%d ' \
                    '--exclude=compute-0-[3-4] --nice ./slurm/parse.sh') % (split_idx, self.digits, 24))
            self.ex(('sbatch --array=%d-%d --ntasks=48 --mem=96G --export=digits=%d,ntasks=%d ' \
                    '--exclude=compute-1-[0-15] --nice ./slurm/parse.sh') % (split_idx+1, self.splits, self.digits, 48))

            logging.info('Luigi will quit now. Monitor the slurm jobs and restart luigi' \
                    ' when they are finished.')
            call('squeue', shell=True)
            exit()
        else:
            raise Exception('Computing cluster not supported!')


class ExportFromTSDB(sciluigi.Task):
    in_parse = None

    outdir = luigi.Parameter(default='data/export')

    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    def out_export(self):
        return [ TargetInfo(self, os.path.join(self.outdir, str(i).zfill(self.digits))) \
                for i in range(0, self.splits) ]

    def run(self):
        self.ex('mkdir -p data/export')
        self.ex('rm log/slurm/* || true')

        self.ex('sbatch --array=0-%d --export=digits=%d,OUTPUT_DIR=%s --nice ./slurm/export.sh' %
                (self.splits, self.digits, self.outdir))

        logging.info('Luigi will quit now. Monitor the slurm jobs and restart luigi' \
                    ' when they are finished.')
        call('squeue', shell=True)
        exit()


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
