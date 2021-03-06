import os
import logging
from subprocess import call
from itertools import accumulate

import luigi
import sciluigi
from sciluigi import TargetInfo

class ParallelData(sciluigi.ExternalTask):
    target = sciluigi.Parameter()
    source = sciluigi.Parameter()

    def out_parallel(self):
        return [ TargetInfo(self, self.source),
                TargetInfo(self, self.target) ]


class PreprocessNematus(sciluigi.Task):
    src_lang = sciluigi.Parameter()
    trg_lang = sciluigi.Parameter()

    in_parallel = None

    def out_processed(self):
        return [ TargetInfo(self, 'data/translate/preprocess/source.tok'),
                TargetInfo(self, 'data/translate/preprocess/target.tok'), 
                TargetInfo(self, 'data/translate/preprocess/source.tok.json'), 
                TargetInfo(self, 'data/translate/preprocess/target.tok.json') ]

    def run(self):
        self.ex('mkdir -p data/translate/preprocess/')

        if False:
            logging.info('Lowercasing and tokenizing source and target data.')
            self.ex('perl nematus/data/lowercase.perl < %s | \
                    perl nematus/data/tokenizer.perl -threads 5 -l %s > data/translate/preprocess/source.tok' \
                    % (self.in_parallel[0].path, self.src_lang))
            self.ex('perl nematus/data/lowercase.perl < %s | \
                    perl nematus/data/tokenizer.perl -threads 5 -l %s > data/translate/preprocess/target.tok.ul' \
                    % (self.in_parallel[1].path, self.trg_lang))

        # underline fix
        self.ex("cat data/translate/preprocess/target.tok.ul | sed 's/ _ /_/g' > \
                data/translate/preprocess/target.tok")

        logging.info('Building vocabularies.')
        self.ex('(. /home/jwei/miniconda3/etc/profile.d/conda.sh && conda activate nematus \
                && python nematus/data/build_dictionary.py \
                data/translate/preprocess/source.tok)')
        self.ex('(. /home/jwei/miniconda3/etc/profile.d/conda.sh && conda activate nematus \
                && python nematus/data/build_dictionary.py \
                data/translate/preprocess/target.tok)')


class TrainDevTestSplits(sciluigi.Task):
    train = luigi.FloatParameter(default=0.87)
    dev = luigi.FloatParameter(default=0.003)
    analysis = luigi.FloatParameter(default=0.127)

    in_processed = None

    def out_splits(self):
        return [ TargetInfo(self, 'data/translate/splits/train.source'), 
                TargetInfo(self, 'data/translate/splits/train.target'), 
                TargetInfo(self, 'data/translate/splits/dev.source'), 
                TargetInfo(self, 'data/translate/splits/dev.target'), 
                TargetInfo(self, 'data/translate/splits/analysis.source'), 
                TargetInfo(self, 'data/translate/splits/analysis.target') ]


    def run(self):
        self.ex('mkdir -p data/translate/splits')
        self.ex('rm data/translate/splits/* || true')

        assert self.train + self.dev + self.analysis == 1.
        lines = sum(1 for line in open(self.in_processed[0].path))
        split_counts = [ 0, int(lines*self.train), int(lines*self.dev), int(lines*self.analysis) ]
        split_idx = list(accumulate(split_counts))
        logging.info('Rough counts of train/dev/analysis sizes: \n\t%s\n\t%s' % \
                (str(split_counts), str(split_idx)))

        tup = tuple(open(out_file.path, 'wt') for out_file in self.out_splits())
        train_src, train_trg, dev_src, dev_trg, analysis_src, analysis_trg = tup

        for i, (src, trg) in enumerate(
                zip(open(self.in_processed[0].path), open(self.in_processed[1].path))):
            if split_idx[1] > i:
                train_src.write(src)
                train_trg.write(trg)
            elif split_idx[2] > i:
                dev_src.write(src)
                dev_trg.write(trg)
            else:
                analysis_src.write(src)
                analysis_trg.write(trg)
        
        [ i.close() for i in tup ]
        call('wc -l data/translate/splits/*', shell=True)


class TrainAttentionSeq2Seq(sciluigi.Task):
    in_train = None

    def out_model(self):
        return TargetInfo(self, './data/translate/models/model.npz.json')


    def run(self):
        self.ex('mkdir -p data/translate/models')
        self.ex('sbatch --wait slurm/train.sh')


class TestTranslations(sciluigi.Task):
    in_model = None

    splits = luigi.IntParameter(default=20)
    digits = luigi.IntParameter(default=4)

    def out_translations(self):
        return TargetInfo(self, 'data/translate/output/translations.out')

    def run(self):
        # make splits
        test_source = 'data/translate/splits/analysis.source'
        self.ex('mkdir -p data/translate/splits/test_splits')
        self.ex('split -d -l $((`wc -l < %s`/%d)) -a %d %s data/translate/splits/test_splits/' %  \
                (test_source, self.splits, self.digits, test_source)) 

        self.ex('mkdir -p data/translate/output/')
        self.ex('mkdir -p data/translate/output/output_splits')
        self.ex('sbatch --array=0-%d --export=digits=%d --nice --wait slurm/translate.sh' % \
                (self.splits, self.digits))

        # concat in order
        self.ex('cat data/translate/output/output_splits/* > data/translate/output/translations.out')


class TrainAndTranslate(sciluigi.WorkflowTask):
    in_data = None

    def out_translations(self):
        return TargetInfo(self, 'data/translate/output/translations.out')

    def workflow(self):
        parallel = self.new_task('Parallel data', ParallelData,
                source='data/pre/shuffled/source',
                target='data/pre/shuffled/target')

        preprocess = self.new_task('Preprocess Nematus', PreprocessNematus,
                src_lang='fr',
                trg_lang='en')
        preprocess.in_parallel = parallel.out_parallel()

        splits = self.new_task('Making train+dev+test splits', TrainDevTestSplits)
        splits.in_processed = preprocess.out_processed()

        train = self.new_task('Train Nematus', TrainAttentionSeq2Seq)
        train.in_train = splits.out_splits()

        translate = self.new_task('Translate with Nematus', TestTranslations)
        translate.in_model = train.out_model()

        return translate


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    luigi.run(main_task_cls=TrainAndTranslate, local_scheduler=True)
