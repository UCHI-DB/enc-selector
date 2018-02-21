INTCOMPRESS.minEnc = min(INTCOMPRESS{:,1:5},[],2);
INTCOMPRESS.minCom = min(INTCOMPRESS{:,6:8},[],2);
INTCOMPRESS.compare = INTCOMPRESS.minEnc./INTCOMPRESS.minCom;

fig=cdfplot(INTCOMPRESS.minEnc);
hold on
cdfplot(INTCOMPRESS.minCom);
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Encoding', 'Compression');
hold off
saveas(fig,'compress_int_cdf.pdf');
close(gcf);
cdfplot(INTCOMPRESS.compare);
xlabel('Encoding / Compression');
ylabel('Percentage');
saveas(gcf, 'compress_int_compare.pdf');
close(gcf);