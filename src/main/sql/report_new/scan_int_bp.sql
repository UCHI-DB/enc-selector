SELECT size.value   size,
       bp.value     bp,
       bpsz.value   bpsz,
       rle.value    rle,
       rlesz.value  rlesz,
       length.value length
FROM col_data cd
       JOIN
     feature size ON size.col_id = cd.id
       AND size.type = 'EncFileSize'
       AND size.name = 'PLAIN_file_size'
       JOIN
     feature bp ON bp.col_id = cd.id
       AND bp.type = 'ScanTimeUsage'
       AND bp.name = 'BP_wallclock'
       JOIN
     feature bpsz on bpsz.col_id = cd.id
       AND bpsz.type = 'EncFileSize'
       AND bpsz.name = 'BP_file_size'
       JOIN
     feature rle ON rle.col_id = cd.id
       AND rle.type = 'ScanTimeUsage'
       AND rle.name = 'RLE_wallclock'
       JOIN
     feature rlesz on rlesz.col_id = cd.id
       AND rlesz.type = 'EncFileSize'
       AND rlesz.name = 'RLE_file_size'
       JOIN
     feature length on length.col_id = cd.id
       AND length.type = 'Length'
       AND length.name = 'mean'
WHERE cd.data_type = 'INTEGER'
  AND size.value > 5000000
  AND cd.parent_id is NULL
  AND cd.id < 15191