SELECT size.value size,
       plain.value plain,
       dict.value dict,
       plaing.value plaing,
       dictg.value dictg,
       plains.value plains,
       dicts.value dicts
FROM col_data cd
       JOIN
     feature size ON size.col_id = cd.id
       AND size.type = 'EncFileSize'
       AND size.name = 'PLAIN_file_size'
       JOIN
     feature dict ON dict.col_id = cd.id
       AND dict.type = 'EncTimeUsage'
       AND dict.name = 'DICT_wctime'
       JOIN
     feature plain ON plain.col_id = cd.id
       AND plain.type = 'EncTimeUsage'
       AND plain.name = 'PLAIN_wctime'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'CompressTimeUsage'
       AND dictg.name = 'DICT_GZIP_wctime'
       JOIN
     feature plaing ON plaing.col_id = cd.id
       AND plaing.type = 'CompressTimeUsage'
       AND plaing.name = 'PLAIN_GZIP_wctime'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'CompressTimeUsage'
       AND dicts.name = 'DICT_LZO_wctime'
       JOIN
     feature plains ON plains.col_id = cd.id
       AND plains.type = 'CompressTimeUsage'
       AND plains.name = 'PLAIN_LZO_wctime'
WHERE cd.data_type = 'DOUBLE'
  AND size.value > 5000000
  AND cd.parent_id is NULL
  AND cd.id < 15530
