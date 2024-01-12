## Forecasting Corporate Bond Returns with a Repeat Sales Model on 72 Distinct, Billion Item Matrices ~ 1TB
As part of FED research with Ivan Ivanov, I looked to estimate returns on different subsets of the corporate bond market over a 12 years period using a repeat sales model derived from a 50 million item dataset. 
This methodology is generally used to construct an index of prices or returns for unique, infrequently traded assets like houses or securities which are likely to be prone to exhibit serial correlation in returns. No one knows true price of a house, you look at houses in same area that look similar to yours that sold for some amount. No two houses are the same. This model eliminates the problem of accounting for return differences in bonds with varying characteristics. 
Data structure key: matrix row is first trade of bond, look at last trade, compute return from the first to the last trade, and estimating the return on the market at that time is a function of all these other bonds in that neighborhood. 

### Data Extraction Methodology: <br>
-Use SLURM to manage and schedule Linux clusters. Computations will be distributed to 36 nodes. <br>
-Start with our massive corporate bond database, use SQL to extract 50 million lines.  <br>
-Segment the dataset where it meets certain rating, maturity, and liquidity constraints, there are 72 separate combinations. Do this via parallelization in R. <br>
-This ended up running faster than running parallelized SQL statements with WHERE conditions.  <br>

### Repeat Sales Model - How it Works: <br>
-Assign 1 for the periods after the first sale until the second sale, and zero otherwise. <br>
-Run a regression on this matrix for 4,000 days and 800,000+ securities, 72 different times for every subset of the market. <br>
-Individual return predictions: there is a coefficient for each date; multiply the coefficients by the time matrix.  <br>
-For individual security/date, sum the cofficents across the row and subtract them from the return on the bond in that time frame. Results in prediction for each row of the return for that security/date.  <br>
-Return on market: model coefficients for each date serve as the return for the market on that date. <br>

### Repeat Sales Model Methodology: <br>
-Chunk and parallelize the data for each bond type and construct a matrix for all days in the 12 year period, about 4000 columns by all CUSIP/dates for that category, around 800,000+ rows. Matrices larger than 8 GB, 1 billion items each. 1 TB of data total for all combinations.  <br>
-Huge matrices made parallelization impractical, circumventing this by writing to a sparse matrix, which vastly reduced the size and allowed take advantage of parallelization. <br>
-Took instead of 15 seconds each so about 20 minutes, to less about 8 minutes with distributed computing. <br>

### Major Bottleneck: <br>
-Can't use standard R’s stats lm.fit() on a matrix with a billion elements.  <br>
-So had to use MatrixModels lm.fit.sparse. <br>
-Noticed coefficients weren’t correct when I was benchmarking the two methods on a subset of the data. <br>
-Upon further investigation, an extremely high VIF and odd looking coefficients prompted me to suspect collinearity.  <br>
-Whereas lm.fit removed collinear columns automatically, the sparse version does not. I had to implement a method of removing the collinear columns. <br>
-Used QR decomposition to find perfectly collinear columns on batches of 100 columns at a time parallelized across 10 cores because runs so slow on 1,000 columns. <br>
-Iterate through, then do one final decomposition on all the noncollinear columns from the smaller batches.  <br>
-Then pass this collinear removed sparse matrix to lm.fit.sparse <br>

### Use: <br>
-Create a dummy for each date for every security having higher than normal returns for their market segments.  <br>
-Used this information to enhance the current model for predicting returns for individual securities. MAE was reduced by around 5% on average. Deemed practically significant. <br>
-Runs quickly, set up an ETL to run daily at market close before other models.  <br>
-Executes in about 8 hours. This allows it to be incorporated in the model ran at market open the next day. <br>

### Why:
-More eyes on it, more context to market by utilizing a longer time frame, trigger discussions so not blindsided.
-Give our narrative to governors before they ask for it
