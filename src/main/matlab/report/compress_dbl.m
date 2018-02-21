DOUBLECOMPRESS.minEnc = min(DOUBLECOMPRESS{:,1:2},[],2);
DOUBLECOMPRESS.minCom = min(DOUBLECOMPRESS{:,3:5},[],2);
DOUBLECOMPRESS.compare = DOUBLECOMPRESS.minEnc./DOUBLECOMPRESS.minCom;

fig=cdfplot(DOUBLECOMPRESS.minEnc);
hold on
cdfplot(DOUBLECOMPRESS.minCom);
cdfplot(INTCOMPRESS.gz);
cdfplot(INTCOMPRESS.lz);
cdfplot(INTCOMPRESS.sn);
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Selected Encoding', 'Best Compression', 'GZip','LZO','Snappy');
hold off
saveas(fig,'compress_dbl_cdf.eps','epsc');
close(gcf);
cdfplot(DOUBLECOMPRESS.compare);
xlabel('Encoding / Compression');
ylabel('Percentage');
saveas(gcf, 'compress_dbl_compare.eps','epsc');
close(gcf);