SELECT size.value     size,
       num.value      num,
       plain.value    plain,
       plaing.value   plaing,
       plaingsz.value plaingsz,
       plains.value   plains,
       plainssz.value plainssz,
       dict.value     dict,
       dictsz.value   dictsz,
       dictg.value    dictg,
       dictgsz.value  dictgsz,
       dicts.value    dicts,
       dictssz.value  dictssz
FROM col_data cd
       JOIN
     feature num on num.col_id = cd.id
       AND num.type = 'Sparsity'
       and num.name = 'count'
       JOIN
     feature size ON size.col_id = cd.id
       AND size.type = 'EncFileSize'
       AND size.name = 'PLAIN_file_size'
       JOIN
     feature plain ON plain.col_id = cd.id
       AND plain.type = 'ScanTimeUsage'
       AND plain.name = 'PLAIN_wallclock'
       JOIN
     feature plaing ON plaing.col_id = cd.id
       AND plaing.type = 'ScanTimeUsage'
       AND plaing.name = 'PLAIN_GZIP_wallclock'
       join
     feature plaingsz on plaingsz.col_id = cd.id
       and plaingsz.type = 'CompressEncFileSize'
       and plaingsz.name = 'PLAIN_GZIP_file_size'
       JOIN
     feature plains ON plains.col_id = cd.id
       AND plains.type = 'ScanTimeUsage'
       AND plains.name = 'PLAIN_LZO_wallclock'
       join
     feature plainssz on plainssz.col_id = cd.id
       and plainssz.type = 'CompressEncFileSize'
       and plainssz.name = 'PLAIN_LZO_file_size'
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
  AND size.value > 1000000
  AND cd.parent_id IS NULL
  AND cd.id < 15191