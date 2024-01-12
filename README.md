## Forecasting Corporate Bond Returns with a Repeat Sales Model on 72 Distinct, Billion Item Matrices ~ 1TB of Data.
As part of FED research with Ivan Ivanov: I looked to estimate returns on different subsets of the corporate bond market over a 12 years period using a repeat sales model derived from a 50 million item dataset. 
This methodology is generally used to construct an index of prices or returns for unique, infrequently traded assets like houses or securities which are likely to be prone to exhibit serial correlation in returns.  
No one knows true price of a house, you look at houses in same area that look similar to yours that sold for some amount. No two houses are the same.  
This model eliminates the problem of accounting for return differences in bonds with varying characteristics. 
Matrix row: first trade of bond, look at last trade, compute return from the first to the last trade, and estimating the return on the market at that time is a function of all these other bonds in that neighborhood. 

Data Extraction Methodology:
-Use SLURM to manage and schedule Linux clusters. Computations will be distributed to 36 nodes.
-Start with our massive corporate bond database, use SQL to extract 50 million lines 
-Segment the dataset where it meets certain rating, maturity, and liquidity constraints, there are 72 seroerate combos. Do this via parallelization in R.
-This ended up running faster than running parallelized SQL statements with WHERE conditions 

Repeat Sales Model - How it works:
-Assign 1 for the periods after the first sale until the second sale, and zero otherwise
-Run a regression on this matrix for 4,000 days and 800,000+ securities, 72 different times for every subset of the market.
-Individual Return Predictions: There is a coefficient for each date. Multiply the coefficients by the time matrix. 
-For individual security/date, sums across the row the cofficents and subtract them from the return on the bond in that time frame. Results in prediction for each row of the return for that security/date 
-Return On Market: model coefficients for each date serve as the return for the market on that date

Repeat Sales Model Methodology:
-Then chunk and parallelize the data for each bond type and construct a matrix for all days in the 12 year period, about 4000 columns by all CUSIP/dates for that category, around 800,000+ rows. Matrices larger than 8 GB, 1 billion items each. 
-Huge matrices made parallelization impractical, circumventing this by writing to a sparse matrix, which vastly reduced the size and allowed take advantage of parallelization.
-Took instead of 15 seconds each so about 20 minutes, to less about 8 minutes with distributed computing.

Major Bottleneck:
-Can't use standard R’s stats lm.fit() on a matrix with a billion elements. 
-So had to use MatrixModels lm.fit.sparse.
-Noticed coefficients weren’t correct when I was benchmarking the two methods on a subset of the data.
-Upon further investigation, an extremely high VIF and odd looking coefficients prompted me to suspect collinearity. 
-Whereas lm.fit removed collinear columns automatically, the sparse version does not. I had to implement a method of removing the collinear columns.
-Used QR decomposition to find perfectly collinear columns on batches of 100 columns at a time parallelized across 10 cores because runs so slow on 1,000 columns.
-Iterate through, then do one final decomposition on all the noncollinear columns from the smaller batches. 
-Then pass this collinear removed sparse matrix to lm.fit.sparse

Use:
-Create a dummy for each date for every security having higher than normal returns for their market segments. 
-Used this information to enhance the current model for predicting returns for individual securities. MAE was reduced by around 2% on average. Deemed practically significant.
-Runs quickly, set up an ETL to run daily at market close before other models. 
-Executes in about 8 hours. This allows it to be incorporated in the model ran at market open the next day.

Why:
-More eyes on it, more context to market by utilizing a longer time frame, trigger discussions so not blindsided.
-Give our narrative to governors before they ask for it
