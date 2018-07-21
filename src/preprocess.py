def preprocess_europarl(src, target, src_out, target_out):
    xml, blank = 0, 0
    with open(src, 'rt') as fr, open(target, 'rt') as en, \
            open(src_out, 'wt') as fr_proc, open(target_out, 'wt') as en_proc:
        try:
            for i, fr_ in enumerate(fr):
                en_ = next(en)
                fr_, en_ = fr_.strip(), en_.strip()

                if fr_.startswith('<') or en_.startswith('<'):
                    xml += 1
                    continue

                if fr_ == '' or en_ == '':
                    blank += 1
                    continue
                
                fr_proc.write('%s\n' % fr_)
                en_proc.write('%s\n' % en_)
        except:
            raise Exception('Corpora are not parallel!')

        return xml, blank


