SELECT cd.name,
       size.value                   size,
       num.value / dict.value       dictspeed,
       num.value / delta.value      deltapeed,
       num.value / deltal.value     deltaspeed,
       num.value / plain.value      plainspeed,
       num.value / dictg.value      dictgspeed,
       num.value / deltag.value     deltagspeed,
       num.value / deltalg.value    deltalgspeed,
       num.value / plaing.value     plaingspeed,
       dictsz.value / size.value    dictsz,

       deltasz.value / size.value   deltasz,
       deltalsz.value / size.value  deltalsz,
       dictgsz.value / size.value   dictgsz,
       deltagsz.value / size.value  deltagsz,
       deltalgsz.value / size.value deltalgsz,
       size.value / size.value      plainsz,
       plaingsz.value / size.value  plaingsz
FROM col_data cd
       join
     feature num on num.col_id = cd.id
       and num.type = 'Sparsity'
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
     feature delta ON delta.col_id = cd.id
       AND delta.type = 'ScanTimeUsage'
       AND delta.name = 'DELTA_wallclock'
       JOIN
     feature deltasz ON deltasz.col_id = cd.id
       AND deltasz.type = 'EncFileSize'
       AND deltasz.name = 'DELTA_file_size'
       JOIN
     feature deltal ON deltal.col_id = cd.id
       AND deltal.type = 'ScanTimeUsage'
       AND deltal.name = 'DELTAL_wallclock'
       JOIN
     feature deltalsz ON deltalsz.col_id = cd.id
       AND deltalsz.type = 'EncFileSize'
       AND deltalsz.name = 'DELTAL_file_size'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'ScanTimeUsage'
       AND dictg.name = 'DICT_GZIP_wallclock'
       JOIN
     feature dictgsz ON dictgsz.col_id = cd.id
       AND dictgsz.type = 'CompressEncFileSize'
       AND dictgsz.name = 'DICT_GZIP_file_size'
       JOIN
     feature deltag ON deltag.col_id = cd.id
       AND deltag.type = 'ScanTimeUsage'
       AND deltag.name = 'DELTA_GZIP_wallclock'
       JOIN
     feature deltagsz ON deltagsz.col_id = cd.id
       AND deltagsz.type = 'CompressEncFileSize'
       AND deltagsz.name = 'DELTA_GZIP_file_size'
       JOIN
     feature deltalg ON deltalg.col_id = cd.id
       AND deltalg.type = 'ScanTimeUsage'
       AND deltalg.name = 'DELTAL_GZIP_wallclock'
       JOIN
     feature deltalgsz ON deltalgsz.col_id = cd.id
       AND deltalgsz.type = 'CompressEncFileSize'
       AND deltalgsz.name = 'DELTAL_GZIP_file_size'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'ScanTimeUsage'
       AND dicts.name = 'DICT_LZO_wallclock'
       JOIN
     feature dictssz ON dictssz.col_id = cd.id
       AND dictssz.type = 'CompressEncFileSize'
       AND dictssz.name = 'DICT_LZO_file_size'
       JOIN
     feature deltas ON deltas.col_id = cd.id
       AND deltas.type = 'ScanTimeUsage'
       AND deltas.name = 'DELTA_LZO_wallclock'
       JOIN
     feature deltassz ON deltassz.col_id = cd.id
       AND deltassz.type = 'CompressEncFileSize'
       AND deltassz.name = 'DELTA_LZO_file_size'
       JOIN
     feature deltals ON deltals.col_id = cd.id
       AND deltals.type = 'ScanTimeUsage'
       AND deltals.name = 'DELTAL_LZO_wallclock'
       JOIN
     feature deltalssz ON deltalssz.col_id = cd.id
       AND deltalssz.type = 'CompressEncFileSize'
       AND deltalssz.name = 'DELTAL_LZO_file_size'
WHERE cd.data_type = 'STRING'
  AND size.value > 10000000
  AND cd.parent_id IS NULL
  AND cd.id < 15191
  AND deltalsz.value < dictsz.value