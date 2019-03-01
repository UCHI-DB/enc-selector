SELECT size.value size,
       plain.value plain,
       dict.value dict,
       delta.value delta,
       deltal.value deltal,
       plaing.value plaing,
       dictg.value dictg,
       deltag.value deltag,
       deltalg.value deltalg,
       plains.value plains,
       dicts.value dicts,
       deltas.value deltas,
       deltals.value deltals
FROM col_data cd
       JOIN
     feature size ON size.col_id = cd.id
       AND size.type = 'EncFileSize'
       AND size.name = 'PLAIN_file_size'
       JOIN
     feature plain ON plain.col_id = cd.id
       AND plain.type = 'EncTimeUsage'
       AND plain.name = 'PLAIN_wctime'
       JOIN
     feature dict ON dict.col_id = cd.id
       AND dict.type = 'EncTimeUsage'
       AND dict.name = 'DICT_wctime'
       JOIN
     feature delta ON delta.col_id = cd.id
       AND delta.type = 'EncTimeUsage'
       AND delta.name = 'DELTA_wctime'
       JOIN
     feature deltal ON deltal.col_id = cd.id
       AND deltal.type = 'EncTimeUsage'
       AND deltal.name = 'DELTAL_wctime'
       JOIN
     feature plaing ON plaing.col_id = cd.id
       AND plaing.type = 'CompressTimeUsage'
       AND plaing.name = 'PLAIN_GZIP_wctime'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'CompressTimeUsage'
       AND dictg.name = 'DICT_GZIP_wctime'
       JOIN
     feature deltag ON deltag.col_id = cd.id
       AND deltag.type = 'CompressTimeUsage'
       AND deltag.name = 'DELTA_GZIP_wctime'
       JOIN
     feature deltalg ON deltalg.col_id = cd.id
       AND deltalg.type = 'CompressTimeUsage'
       AND deltalg.name = 'DELTAL_GZIP_wctime'
       JOIN
     feature plains ON plains.col_id = cd.id
       AND plains.type = 'CompressTimeUsage'
       AND plains.name = 'PLAIN_LZO_wctime'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'CompressTimeUsage'
       AND dicts.name = 'DICT_LZO_wctime'
       JOIN
     feature deltas ON deltas.col_id = cd.id
       AND deltas.type = 'CompressTimeUsage'
       AND deltas.name = 'DELTA_LZO_wctime'
       JOIN
     feature deltals ON deltals.col_id = cd.id
       AND deltals.type = 'CompressTimeUsage'
       AND deltals.name = 'DELTAL_LZO_wctime'
WHERE cd.data_type = 'STRING'
  AND size.value > 10000000
  AND cd.parent_id is NULL
  AND cd.id < 15530
