STRCOMPRESS.minEnc = min(STRCOMPRESS{:,1:4},[],2);
STRCOMPRESS.minCom = min(STRCOMPRESS{:,5:7},[],2);
STRCOMPRESS.compare = STRCOMPRESS.minEnc./STRCOMPRESS.minCom;

fig=cdfplot(STRCOMPRESS.minEnc);
hold on
cdfplot(STRCOMPRESS.minCom);
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Encoding', 'Compression');
hold off
saveas(fig,'compress_str_cdf.pdf');
close(gcf);
cdfplot(STRCOMPRESS.compare);
xlabel('Encoding / Compression');
ylabel('Percentage');
saveas(gcf, 'compress_str_compare.pdf');
close(gcf);