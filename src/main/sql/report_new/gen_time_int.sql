SELECT size.value size,
       plain.value plain,
       dict.value dict,
       bp.value bp,
       rle.value rle,
       deltabp.value deltabp,
       plaing.value plaing,
       dictg.value dictg,
       bpg.value bpg,
       rleg.value rleg,
       deltabpg.value deltabpg,
       plains.value plains,
       dicts.value dicts,
       bps.value bps,
       rles.value rles,
       deltabps.value deltabps
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
     feature bp ON bp.col_id = cd.id
       AND bp.type = 'EncTimeUsage'
       AND bp.name = 'BP_wctime'
       JOIN
     feature rle ON rle.col_id = cd.id
       AND rle.type = 'EncTimeUsage'
       AND rle.name = 'RLE_wctime'
       JOIN
     feature deltabp ON deltabp.col_id = cd.id
       AND deltabp.type = 'EncTimeUsage'
       AND deltabp.name = 'DELTABP_wctime'
       JOIN
     feature plaing ON plaing.col_id = cd.id
       AND plaing.type = 'CompressTimeUsage'
       AND plaing.name = 'PLAIN_GZIP_wctime'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'CompressTimeUsage'
       AND dictg.name = 'DICT_GZIP_wctime'
       JOIN
     feature bpg ON bpg.col_id = cd.id
       AND bpg.type = 'CompressTimeUsage'
       AND bpg.name = 'BP_GZIP_wctime'
       JOIN
     feature rleg ON rleg.col_id = cd.id
       AND rleg.type = 'CompressTimeUsage'
       AND rleg.name = 'RLE_GZIP_wctime'
       JOIN
     feature deltabpg ON deltabpg.col_id = cd.id
       AND deltabpg.type = 'CompressTimeUsage'
       AND deltabpg.name = 'DELTABP_GZIP_wctime'
       JOIN
     feature plains ON plains.col_id = cd.id
       AND plains.type = 'CompressTimeUsage'
       AND plains.name = 'PLAIN_LZO_wctime'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'CompressTimeUsage'
       AND dicts.name = 'DICT_LZO_wctime'
       JOIN
     feature bps ON bps.col_id = cd.id
       AND bps.type = 'CompressTimeUsage'
       AND bps.name = 'BP_LZO_wctime'
       JOIN
     feature rles ON rles.col_id = cd.id
       AND rles.type = 'CompressTimeUsage'
       AND rles.name = 'RLE_LZO_wctime'
       JOIN
     feature deltabps ON deltabps.col_id = cd.id
       AND deltabps.type = 'CompressTimeUsage'
       AND deltabps.name = 'DELTABP_LZO_wctime'
WHERE cd.data_type = 'INTEGER'
  AND size.value > 5000000
  AND cd.parent_id is NULL
  AND cd.id < 15191
