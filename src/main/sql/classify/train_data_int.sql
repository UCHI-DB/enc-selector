SELECT 
    f1.value,
    f2.value,
    f3.value,
    f4.value,
    f5.value,
    f6.value,
    f7.value,
    f8.value,
    f9.value,
    f10.value,
    f11.value,
    f12.value,
    f13.value,
    f14.value,
    f15.value,
    f16.value,
    f17.value,
    f18.value,
    f19.value,
    f20.value,
    (CASE label.name
        WHEN 'PLAIN_file_size' THEN 0
        WHEN 'DICT_file_size' THEN 1
        WHEN 'RLE_file_size' THEN 2
        WHEN 'BP_file_size' THEN 3
        WHEN 'DELTABP_file_size' THEN 4
    END) L
FROM
    col_data cd
        JOIN
    feature f1 ON f1.col_id = cd.id
        AND f1.type = 'Distinct'
        AND f1.name = 'ratio'
        JOIN
    feature f2 ON f2.col_id = cd.id
        AND f2.type = 'Entropy'
        AND f2.name = 'line_max'
        JOIN
    feature f3 ON f3.col_id = cd.id
        AND f3.type = 'Entropy'
        AND f3.name = 'line_min'
        JOIN
    feature f4 ON f4.col_id = cd.id
        AND f4.type = 'Entropy'
        AND f4.name = 'line_mean'
        JOIN
    feature f5 ON f5.col_id = cd.id
        AND f5.type = 'Entropy'
        AND f5.name = 'line_var'
        JOIN
    feature f6 ON f6.col_id = cd.id
        AND f6.type = 'Entropy'
        AND f6.name = 'total'
        JOIN
    feature f7 ON f7.col_id = cd.id AND f7.type = 'Length'
        AND f7.name = 'max'
        JOIN
    feature f8 ON f8.col_id = cd.id AND f8.type = 'Length'
        AND f8.name = 'min'
        JOIN
    feature f9 ON f9.col_id = cd.id AND f9.type = 'Length'
        AND f9.name = 'mean'
        JOIN
    feature f10 ON f10.col_id = cd.id
        AND f10.type = 'Length'
        AND f10.name = 'variance'
        JOIN
    feature f11 ON f11.col_id = cd.id
        AND f11.type = 'Sparsity'
        AND f11.name = 'valid_ratio'
        JOIN
    feature f12 ON f12.col_id = cd.id
        AND f12.type = 'Sortness'
        AND f12.name = 'ivpair_50'
        JOIN
    feature f13 ON f13.col_id = cd.id
        AND f13.type = 'Sortness'
        AND f13.name = 'ivpair_100'
        JOIN
    feature f14 ON f14.col_id = cd.id
        AND f14.type = 'Sortness'
        AND f14.name = 'ivpair_200'
        JOIN
    feature f15 ON f15.col_id = cd.id
        AND f15.type = 'Sortness'
        AND f15.name = 'kendalltau_50'
        JOIN
    feature f16 ON f16.col_id = cd.id
        AND f16.type = 'Sortness'
        AND f16.name = 'kendalltau_100'
        JOIN
    feature f17 ON f17.col_id = cd.id
        AND f17.type = 'Sortness'
        AND f17.name = 'kendalltau_200'
        JOIN
    feature f18 ON f18.col_id = cd.id
        AND f18.type = 'Sortness'
        AND f18.name = 'spearmanrho_50'
        JOIN
    feature f19 ON f19.col_id = cd.id
        AND f19.type = 'Sortness'
        AND f19.name = 'spearmanrho_100'
        JOIN
    feature f20 ON f20.col_id = cd.id
        AND f20.type = 'Sortness'
        AND f20.name = 'spearmanrho_200'
        JOIN
    feature label ON label.col_id = cd.id
        AND label.type IN ('EncFileSize' , 'CompressEncFileSize')
        AND label.value = (SELECT 
            MIN(value)
        FROM
            feature enc
        WHERE
            enc.col_id = cd.id
                AND enc.type IN ('EncFileSize' , 'CompressEncFileSize')
                AND enc.name <> 'BITVECTOR_file_size'
                AND enc.value > 0)
WHERE
    cd.data_type = 'INTEGER'
LIMIT 50000