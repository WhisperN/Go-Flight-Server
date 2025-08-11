import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load Tab separated file
df = pd.read_csv("../data/sPlot.tsv", sep="\t")

# Transform into arrow format
table = pa.Table.from_pandas(df)

# Save to parquet
pq.write_table(table, "../../data/sPlot_CWM_CWV.parquet")