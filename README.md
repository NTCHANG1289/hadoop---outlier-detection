# hadoop---outlier-detection
The code is for Distance-Based Outlier Detection Clustering. If a point with lesss than k points that are within r distance,
then report the point as an outlier.
The setUp part is used to define the r - radius and k - counts.
The mapper split the dataset into n*n blocks, and distinguish the zone or supporting area of points.
The reducer then start calculating the distances between points and report outliers in each zone.
The code is worked by Nai-tan Chang.
