import os
import logging
import datetime
from subprocess import call

import luigi, sciluigi
from sciluigi import TargetInfo

class Text(sciluigi.ExternalTask):
    text = luigi.Parameter()

    def out_data(self):
        return TargetInfo(self, self.text)


# TODO filter english sentences to parse based on
# sentences that are already parsed...
# read in this data from the export folder
class FilterParsed(sciluigi.Task):
    pass


class Split(sciluigi.Task):
    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    in_data = None

    def out_splits(self):
        return [ TargetInfo(self, 'data/parse/splits/%s' % ('%d' % i).zfill(self.digits)) \
                for i in range(0, self.splits) ]

    def run(self):
        self.ex('mkdir -p data/parse/splits/')
        self.ex('split -d -l $((`wc -l < %s`/%d)) -a %d %s data/parse/splits/' %  \
                (self.in_data.path, self.splits, self.digits, self.in_data.path)) 


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
        #self.ex('rm log/slurm/* || true')

        if self.compute == 'blake':
            # The following code is specific to blake2.cs.umass.edu
            # as there are 24 and 48 core CPU nodes, and each logon
            # parse script can only be run on one node at a time
            split_idx = int(self.splits * 0.85)

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

    nrun = luigi.Parameter()
    outdir = luigi.Parameter()

    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    def out_export(self):
        return [ TargetInfo(self, os.path.join('data/export', self.nrun, str(i).zfill(self.digits))) \
                for i in range(0, self.splits) ]

    def run(self):
        self.ex('mkdir -p data/export')
        self.ex('mkdir -p %s' % os.path.join('data/export', self.nrun))

        self.ex('sbatch --array=0-%d --export=digits=%d,OUTPUT_DIR=%s --nice --wait ./slurm/export.sh' %
                (self.splits, self.digits, self.outdir))


class Parse(sciluigi.WorkflowTask):
    in_text = None

    nrun = luigi.Parameter(default=datetime.datetime.now().strftime("%Y-%m-%d"))
    splits = luigi.IntParameter()
    digits = luigi.IntParameter(default=4)

    def out_export(self):
        return [ TargetInfo(self, os.path.join('data/export', self.nrun, str(i).zfill(self.digits))) \
                for i in range(0, self.splits) ]

    def workflow(self):
        splits = self.splits
        split = self.new_task('Split', Split,
                splits=splits)
        split.in_data = self.in_text

        parse = self.new_task('Parse input with PET', ParseWithPET,
                splits=splits)
        parse.in_splits = split.out_splits()

        export = self.new_task('Export parses', ExportFromTSDB,
                splits=splits,
                nrun=self.nrun,
                outdir=os.path.join('data/export', self.nrun))
        export.in_parse = parse.out_parse()

        return export


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    luigi.run(main_task_cls=Parse, local_scheduler=True)
