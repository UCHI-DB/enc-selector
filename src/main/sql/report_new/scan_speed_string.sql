SELECT 
    size.value,
    bp.value,
    rle.value,
    dict.value,
    deltabp.value,
    bpg.value,
    rleg.value,
    dictg.value,
    deltabpg.value,
    bps.value,
    rles.value,
    dicts.value,
    deltabps.value
FROM
    col_data cd
        JOIN
    feature size ON size.col_id = cd.id
        AND size.type = 'EncFileSize'
        AND size.name = 'PLAIN_file_size'
        JOIN
    feature bp ON bp.col_id = cd.id
        AND bp.type = 'ScanTimeUsage'
        AND bp.name = 'BP_wallclock'
        JOIN
    feature rle ON rle.col_id = cd.id
        AND rle.type = 'ScanTimeUsage'
        AND rle.name = 'RLE_wallclock'
        JOIN
    feature dict ON dict.col_id = cd.id
        AND dict.type = 'ScanTimeUsage'
        AND dict.name = 'DICT_wallclock'
        JOIN
    feature deltabp ON deltabp.col_id = cd.id
        AND deltabp.type = 'ScanTimeUsage'
        AND deltabp.name = 'DELTABP_wallclock'
        JOIN
    feature bpg ON bpg.col_id = cd.id
        AND bpg.type = 'ScanTimeUsage'
        AND bpg.name = 'BP_GZIP_wallclock'
        JOIN
    feature rleg ON rleg.col_id = cd.id
        AND rleg.type = 'ScanTimeUsage'
        AND rleg.name = 'RLE_GZIP_wallclock'
        JOIN
    feature dictg ON dictg.col_id = cd.id
        AND dictg.type = 'ScanTimeUsage'
        AND dictg.name = 'DICT_GZIP_wallclock'
        JOIN
    feature deltabpg ON deltabpg.col_id = cd.id
        AND deltabpg.type = 'ScanTimeUsage'
        AND deltabpg.name = 'DELTABP_GZIP_wallclock'
        JOIN
    feature bps ON bps.col_id = cd.id
        AND bps.type = 'ScanTimeUsage'
        AND bps.name = 'BP_LZO_wallclock'
        JOIN
    feature rles ON rles.col_id = cd.id
        AND rles.type = 'ScanTimeUsage'
        AND rles.name = 'RLE_LZO_wallclock'
        JOIN
    feature dicts ON dicts.col_id = cd.id
        AND dicts.type = 'ScanTimeUsage'
        AND dicts.name = 'DICT_LZO_wallclock'
        JOIN
    feature deltabps ON deltabps.col_id = cd.id
        AND deltabps.type = 'ScanTimeUsage'
        AND deltabps.name = 'DELTABP_LZO_wallclock'
WHERE
    cd.data_type = 'INTEGER'
        AND size.value > 10000000