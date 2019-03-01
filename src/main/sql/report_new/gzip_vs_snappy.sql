select gzip.value                gzip,
       snappy.value              snappy,
       gzip.value / snappy.value rate,
       length.value              minl,
       length2.value             maxl,
       entropy.value             line,
       entropy2.value            totale,
       size.value                size
from col_data cd
       join feature gzip
            on gzip.col_id = cd.id and gzip.type = 'ScanTimeUsage' and gzip.name = 'PLAIN_GZIP_wallclock'
       join feature snappy
            on snappy.col_id = cd.id and snappy.type = 'ScanTimeUsage' and snappy.name = 'PLAIN_LZO_wallclock'
       join feature length
            on length.col_id = cd.id and length.type = 'Length' and length.name = 'mean'
       join feature length2
            on length2.col_id = cd.id and length2.type = 'Length' and length2.name = 'max'
       join feature entropy
            on entropy.col_id = cd.id and entropy.type = 'Entropy' and entropy.name = 'line_mean'
       join feature entropy2
            on entropy2.col_id = cd.id and entropy2.type = 'Entropy' and entropy2.name = 'total'
       join feature size on size.col_id = cd.id and size.type = 'EncFileSize' and size.name = 'PLAIN_file_size'
where size.value > 10000000
  and cd.parent_id is NULL
  and cd.id < 15144
  and cd.data_type = 'INTEGER'