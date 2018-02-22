% value 

threshold = 8;
value2 = value(value<threshold);

[b,a] = histcounts(value,150);
b(151) = b(150);
data = [a',b'];
csvwrite('subattr_hist.data',data);

cdfplot(value);
title('');
xlabel('Ratio');
ylabel('Percentage');