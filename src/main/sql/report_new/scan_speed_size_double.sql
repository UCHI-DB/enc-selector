SELECT size.value,
       dict.value,
       dictsz.value,
       dictg.value,
       dictgsz.value,
       dicts.value,
       dictssz.value
FROM col_data cd
       JOIN
     feature size ON size.col_id = cd.id
       AND size.type = 'EncFileSize'
       AND size.name = 'PLAIN_file_size'
       JOIN
     feature dict ON dict.col_id = cd.id
       AND dict.type = 'ScanTimeUsage'
       AND dict.name = 'DICT_wallclock'
       JOIN
     feature dictsz ON dictsz.col_id = cd.id
       AND dictsz.type = 'EncFileSize'
       AND dictsz.name = 'DICT_file_size'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'ScanTimeUsage'
       AND dictg.name = 'DICT_GZIP_wallclock'
       JOIN
     feature dictgsz ON dictgsz.col_id = cd.id
       AND dictgsz.type = 'CompressEncFileSize'
       AND dictgsz.name = 'DICT_GZIP_file_size'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'ScanTimeUsage'
       AND dicts.name = 'DICT_LZO_wallclock'
       JOIN
     feature dictssz ON dictssz.col_id = cd.id
       AND dictssz.type = 'CompressEncFileSize'
       AND dictssz.name = 'DICT_LZO_file_size'
WHERE cd.data_type = 'DOUBLE'
  AND size.value
  > 1000000