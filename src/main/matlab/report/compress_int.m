minEnc = min(INTCOMPRESS{:,1:5},[],2);
minCom = min(INTCOMPRESS{:,6:8},[],2);
compare = minCom./minEnc;

cdfplot(minEnc);
hold on
h2=cdfplot(minCom);
set(h2, 'LineStyle','--','color','r');
h3=cdfplot(INTCOMPRESS.gz);
set(h3, 'LineStyle',':','color','b');
cdfplot(INTCOMPRESS.lz);
cdfplot(INTCOMPRESS.sn);
title('');
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Selected Encoding', 'Best Compression', 'GZip','LZO','Snappy','Location','southeast');
hold off
saveas(gcf,'compress_int_cdf.eps','epsc');
close(gcf);

threshold = 5;

bc=compare;
bc(bc>threshold)=threshold;
h1=cdfplot(bc);
set(h1, 'LineStyle','--','color','r');
hold on
gzc = INTCOMPRESS.gz./minEnc;
gzc(gzc>threshold)=threshold;
h2=cdfplot(gzc);
set(h2, 'LineStyle',':','color','b');
lzc = INTCOMPRESS.lz./minEnc;
lzc(lzc>threshold)=threshold;
cdfplot(lzc);
snc = INTCOMPRESS.sn./minEnc;
snc(snc>threshold)=threshold;
cdfplot(snc);
title('');
xlabel('Compression / Encoding Ratio');
ylabel('Percentage');
legend('Best Compression', 'GZip','LZO','Snappy','Location','southeast');
saveas(gcf, 'compress_int_compare.eps','epsc');
close(gcf);