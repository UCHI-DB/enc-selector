STRCOMPRESS.minEnc = min(STRCOMPRESS{:,1:4},[],2);
STRCOMPRESS.minCom = min(STRCOMPRESS{:,5:7},[],2);
STRCOMPRESS.compare = STRCOMPRESS.minEnc./STRCOMPRESS.minCom;

fig=cdfplot(STRCOMPRESS.minEnc);
hold on
cdfplot(STRCOMPRESS.minCom);
cdfplot(INTCOMPRESS.gz);
cdfplot(INTCOMPRESS.lz);
cdfplot(INTCOMPRESS.sn);
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Selected Encoding', 'Best Compression', 'GZip','LZO','Snappy');
hold off
saveas(fig,'compress_str_cdf.eps','epsc');
close(gcf);
cdfplot(STRCOMPRESS.compare);
xlabel('Encoding / Compression');
ylabel('Percentage');
saveas(gcf, 'compress_str_compare.eps','epsc');
close(gcf);