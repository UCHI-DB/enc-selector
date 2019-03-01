SELECT size.value size,
       plain.value plain,
       plaing.value plaing,
       plains.value plains
FROM col_data cd
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
       JOIN
     feature plains ON plains.col_id = cd.id
       AND plains.type = 'ScanTimeUsage'
       AND plains.name = 'PLAIN_LZO_wallclock'
WHERE cd.data_type = 'DOUBLE'
  AND size.value > 5000000
  AND cd.parent_id is NULL
  AND cd.id < 15191