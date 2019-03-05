SELECT cd.name,
       size.value                   size,
       num.value / bp.value         bpspeed,
       num.value / bpg.value         bpgspeed,
       num.value / rle.value        rlespeed,
       num.value / rleg.value        rlegspeed,
       num.value / dict.value       dictspeed,
       num.value / dictg.value       dictgspeed,
       num.value / deltabp.value    dbpspeed,
       num.value / deltabpg.value    dbpgspeed,
       bpsz.value / size.value      bpsz,
       rlesz.value / size.value     rlesz,
       dictsz.value / size.value    dictsz,
       deltabpsz.value / size.value deltabpsz,
       bpgsz.value / size.value      bpgsz,
       rlegsz.value / size.value     rlegsz,
       dictgsz.value / size.value    dictgsz,
       deltabpgsz.value / size.value deltabpgsz
FROM col_data cd
       JOIN
     feature num on num.col_id = cd.id
       AND num.type = 'Sparsity'
       AND num.name = 'count'
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
     feature dict ON dict.col_id = cd.id
       AND dict.type = 'ScanTimeUsage'
       AND dict.name = 'DICT_wallclock'
       JOIN
     feature dictsz ON dictsz.col_id = cd.id
       AND dictsz.type = 'EncFileSize'
       AND dictsz.name = 'DICT_file_size'
       JOIN
     feature deltabp ON deltabp.col_id = cd.id
       AND deltabp.type = 'ScanTimeUsage'
       AND deltabp.name = 'DELTABP_wallclock'
       JOIN
     feature deltabpsz on deltabpsz.col_id = cd.id
       AND deltabpsz.type = 'EncFileSize'
       AND deltabpsz.name = 'DELTABP_file_size'
       JOIN
     feature bpg ON bpg.col_id = cd.id
       AND bpg.type = 'ScanTimeUsage'
       AND bpg.name = 'BP_GZIP_wallclock'
       JOIN
     feature bpgsz on bpgsz.col_id = cd.id
       AND bpgsz.type = 'CompressEncFileSize'
       AND bpgsz.name = 'BP_GZIP_file_size'
       JOIN
     feature rleg ON rleg.col_id = cd.id
       AND rleg.type = 'ScanTimeUsage'
       AND rleg.name = 'RLE_GZIP_wallclock'
       JOIN
     feature rlegsz on rlegsz.col_id = cd.id
       AND rlegsz.type = 'CompressEncFileSize'
       AND rlegsz.name = 'RLE_GZIP_file_size'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'ScanTimeUsage'
       AND dictg.name = 'DICT_GZIP_wallclock'
       JOIN
     feature dictgsz on dictgsz.col_id = cd.id
       AND dictgsz.type = 'CompressEncFileSize'
       AND dictgsz.name = 'DICT_GZIP_file_size'
       JOIN
     feature deltabpg ON deltabpg.col_id = cd.id
       AND deltabpg.type = 'ScanTimeUsage'
       AND deltabpg.name = 'DELTABP_GZIP_wallclock'
       JOIN
     feature deltabpgsz on deltabpgsz.col_id = cd.id
       AND deltabpgsz.type = 'CompressEncFileSize'
       AND deltabpgsz.name = 'DELTABP_GZIP_file_size'
       JOIN
     feature bps ON bps.col_id = cd.id
       AND bps.type = 'ScanTimeUsage'
       AND bps.name = 'BP_LZO_wallclock'
       JOIN
     feature bpssz on bpssz.col_id = cd.id
       AND bpssz.type = 'CompressEncFileSize'
       AND bpssz.name = 'BP_LZO_file_size'
       JOIN
     feature rles ON rles.col_id = cd.id
       AND rles.type = 'ScanTimeUsage'
       AND rles.name = 'RLE_LZO_wallclock'
       JOIN
     feature rlessz on rlessz.col_id = cd.id
       AND rlessz.type = 'CompressEncFileSize'
       AND rlessz.name = 'RLE_LZO_file_size'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'ScanTimeUsage'
       AND dicts.name = 'DICT_LZO_wallclock'
       JOIN
     feature dictssz on dictssz.col_id = cd.id
       AND dictssz.type = 'CompressEncFileSize'
       AND dictssz.name = 'DICT_LZO_file_size'
       JOIN
     feature deltabps ON deltabps.col_id = cd.id
       AND deltabps.type = 'ScanTimeUsage'
       AND deltabps.name = 'DELTABP_LZO_wallclock'
       JOIN
     feature deltabpssz on deltabpssz.col_id = cd.id
       AND deltabpssz.type = 'CompressEncFileSize'
       AND deltabpssz.name = 'DELTABP_LZO_file_size'
WHERE cd.data_type = 'INTEGER'
  AND size.value > 5000000
  AND cd.parent_id is NULL
  AND cd.id < 15191
  AND deltabpsz.value < rlesz.value
  and deltabpsz.value < dictsz.value